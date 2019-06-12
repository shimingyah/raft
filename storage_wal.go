// Copyright 2019 shimingyah. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package raft

import (
	"bytes"
	"encoding/binary"
	"errors"
	"math"
	"sync"

	pb "github.com/shimingyah/raft/raftpb"
	"github.com/shimingyah/wisckey"
)

var errNotFound = errors.New("Unable to find raft entry")

type txnUnifier struct {
	txn *wisckey.Txn
	db  *wisckey.DB
}

func (ds *DiskStorage) newTxnUnifier() *txnUnifier {
	return &txnUnifier{txn: ds.db.NewTransaction(true), db: ds.db}
}

func (tu *txnUnifier) Set(key, val []byte) error {
	return tu.run(func() error {
		return tu.txn.Set(key, val)
	})
}

func (tu *txnUnifier) Delete(key []byte) error {
	return tu.run(func() error {
		return tu.txn.Delete(key)
	})
}

func (tu *txnUnifier) run(fn func() error) error {
	err := fn()
	// error can be nil, we can return here.
	// if txn too big, this fn will not append to txn.
	if err != wisckey.ErrTxnTooBig {
		return err
	}

	// commit big txn and create new txn to op fn.
	if err := tu.txn.Commit(); err != nil {
		return err
	}
	tu.txn = tu.db.NewTransaction(true)

	// reexec fn
	return fn()
}

func (tu *txnUnifier) Done() error {
	return tu.txn.Commit()
}

func (tu *txnUnifier) Cancel() {
	tu.txn.Discard()
}

// cache the first index and snapshot in mem.
type cache struct {
	firstIndex uint64
	snapshot   pb.Snapshot
	sync.RWMutex
}

func (c *cache) setFirstIndex(first uint64) {
	c.Lock()
	defer c.Unlock()
	c.firstIndex = first
}

func (c *cache) getFirstIndex() uint64 {
	c.RLock()
	defer c.RUnlock()
	return c.firstIndex
}

func (c *cache) setSnapshot(s pb.Snapshot) {
	c.Lock()
	defer c.Unlock()
	c.firstIndex = 0
	c.snapshot = s
}

func (c *cache) getSnapshot() pb.Snapshot {
	c.RLock()
	defer c.RUnlock()
	return c.snapshot
}

// DiskStorage implements the Storage interface backed by wisckey
type DiskStorage struct {
	// rid raft id
	rid uint64

	// gid group id
	gid uint32

	// cache snaphot and first index
	cache cache

	// db persistent storage raft log
	db *wisckey.DB
}

// NewDiskStorage create diskStorage
func NewDiskStorage(db *wisckey.DB, rid uint64, gid uint32) *DiskStorage {
	ds := &DiskStorage{db: db, rid: rid, gid: gid}
	Check(ds.storeRaftID(rid))

	// return exists raft log
	snap, err := ds.Snapshot()
	Check(err)
	if !IsEmptySnap(snap) {
		return ds
	}

	// init a new raft log
	_, err = ds.FirstIndex()
	if err == errNotFound {
		ents := make([]pb.Entry, 1)
		Check(ds.reset(ents))
	} else {
		Check(err)
	}

	return ds
}

// InitialState see Storage interface.
func (ds *DiskStorage) InitialState() (hs pb.HardState, cs pb.ConfState, err error) {
	hs, err = ds.HardState()
	if err != nil {
		return
	}

	var snap pb.Snapshot
	snap, err = ds.Snapshot()
	if err != nil {
		return
	}

	return hs, snap.Metadata.ConfState, nil
}

// Entries see Storage interface.
func (ds *DiskStorage) Entries(lo, hi, maxSize uint64) ([]pb.Entry, error) {
	first, err := ds.FirstIndex()
	if err != nil {
		return nil, err
	}
	if lo < first {
		return nil, ErrCompacted
	}

	last, err := ds.LastIndex()
	if err != nil {
		return nil, err
	}
	if hi > last+1 {
		return nil, ErrUnavailable
	}

	return ds.allEntries(lo, hi, maxSize)
}

// Term see Storage interface.
// [FirstIndex()-1, LastIndex()]: see FirstIndex
// the FirstIndex()-1 is empty log entry.
func (ds *DiskStorage) Term(idx uint64) (uint64, error) {
	first, err := ds.FirstIndex()
	if err != nil {
		return 0, err
	}
	if idx < first-1 {
		return 0, ErrCompacted
	}

	var e pb.Entry
	if _, err := ds.seekEntry(&e, idx, false); err == errNotFound {
		return 0, ErrUnavailable
	} else if err != nil {
		return 0, err
	}

	if idx < e.Index {
		return 0, ErrCompacted
	}
	return e.Term, nil
}

// FirstIndex see Storage interface.
// index + 1: leader will commit an empty log entry when it's selected.
// it will avoid commit from previous leader log entry.
// see https://zhuanlan.zhihu.com/p/40175038
func (ds *DiskStorage) FirstIndex() (uint64, error) {
	snap := ds.cache.getSnapshot()
	if !IsEmptySnap(snap) {
		return snap.Metadata.Index + 1, nil
	}
	if first := ds.cache.getFirstIndex(); first > 0 {
		return first, nil
	}

	index, err := ds.seekEntry(nil, 0, false)
	if err == nil {
		ds.cache.setFirstIndex(index + 1)
	}

	return index + 1, err
}

// LastIndex see Storage interface.
func (ds *DiskStorage) LastIndex() (uint64, error) {
	return ds.seekEntry(nil, math.MaxUint64, true)
}

// Snapshot see Storage interface.
func (ds *DiskStorage) Snapshot() (snap pb.Snapshot, err error) {
	if s := ds.cache.getSnapshot(); !IsEmptySnap(s) {
		return s, nil
	}
	err = ds.db.View(func(txn *wisckey.Txn) error {
		item, err := txn.Get(ds.snapshotKey())
		if err != nil {
			return err
		}
		return item.Value(func(val []byte) error {
			return snap.Unmarshal(val)
		})
	})
	if err == wisckey.ErrKeyNotFound {
		return snap, nil
	}
	return snap, err
}

// RaftID return current raft id
func (ds *DiskStorage) RaftID() (id uint64, err error) {
	err = ds.db.View(func(txn *wisckey.Txn) error {
		item, err := txn.Get(raftIDKey)
		if err != nil {
			return err
		}
		return item.Value(func(val []byte) error {
			id = binary.BigEndian.Uint64(val)
			return nil
		})
	})
	if err == wisckey.ErrKeyNotFound {
		return 0, nil
	}
	return id, err
}

// HardState return the raft group hard state
func (ds *DiskStorage) HardState() (hs pb.HardState, err error) {
	err = ds.db.View(func(txn *wisckey.Txn) error {
		item, err := txn.Get(ds.hardStateKey())
		if err != nil {
			return err
		}
		return item.Value(func(val []byte) error {
			return hs.Unmarshal(val)
		})
	})
	if err == wisckey.ErrKeyNotFound {
		return hs, nil
	}
	return hs, err
}

// NumEntries return all of number entries
func (ds *DiskStorage) NumEntries() (int, error) {
	var count int
	err := ds.db.View(func(txn *wisckey.Txn) error {
		opt := wisckey.DefaultIteratorOptions
		opt.PrefetchValues = false
		itr := txn.NewIterator(opt)
		defer itr.Close()

		start := ds.entryKey(0)
		prefix := ds.entryPrefix()
		for itr.Seek(start); itr.ValidForPrefix(prefix); itr.Next() {
			count++
		}
		return nil
	})
	return count, err
}

// CreateSnapshot from first index to i, [first, i]
func (ds *DiskStorage) CreateSnapshot(i uint64, cs *pb.ConfState, data []byte) error {
	first, err := ds.FirstIndex()
	if err != nil {
		return err
	}
	if i < first {
		return ErrSnapOutOfDate
	}

	var e pb.Entry
	if _, err := ds.seekEntry(&e, i, false); err != nil {
		return err
	}
	if e.Index != i {
		return errNotFound
	}

	AssertTrue(cs != nil)
	var snap pb.Snapshot
	snap.Metadata.Index = i
	snap.Metadata.Term = e.Term
	snap.Metadata.ConfState = *cs
	snap.Data = data

	u := ds.newTxnUnifier()
	defer u.Cancel()

	if err := ds.setSnapshot(u, snap); err != nil {
		return err
	}
	if err := ds.deleteUntil(u, snap.Metadata.Index); err != nil {
		return err
	}
	return u.Done()
}

// Save would write Entries, HardState and Snapshot to persistent storage in order, i.e. Entries
// first, then HardState and Snapshot if they are not empty. If persistent storage supports atomic
// writes then all of them can be written together. Note that when writing an Entry with Index i,
// any previously-persisted entries with Index >= i must be discarded.
func (ds *DiskStorage) Save(h pb.HardState, es []pb.Entry, snap pb.Snapshot) error {
	u := ds.newTxnUnifier()
	defer u.Cancel()

	if err := ds.appendEntries(u, es); err != nil {
		return err
	}
	if err := ds.setHardState(u, h); err != nil {
		return err
	}
	if err := ds.setSnapshot(u, snap); err != nil {
		return err
	}
	return u.Done()
}

var raftIDKey = []byte("raftID")

func (ds *DiskStorage) hardStateKey() []byte {
	b := make([]byte, 14)
	binary.BigEndian.PutUint64(b[0:8], ds.rid)
	copy(b[8:10], []byte("hs"))
	binary.BigEndian.PutUint32(b[10:14], ds.gid)
	return b
}

func (ds *DiskStorage) snapshotKey() []byte {
	b := make([]byte, 14)
	binary.BigEndian.PutUint64(b[0:8], ds.rid)
	copy(b[8:10], []byte("ss"))
	binary.BigEndian.PutUint32(b[10:14], ds.gid)
	return b
}

func (ds *DiskStorage) entryKey(idx uint64) []byte {
	b := make([]byte, 20)
	binary.BigEndian.PutUint64(b[0:8], ds.rid)
	binary.BigEndian.PutUint32(b[8:12], ds.gid)
	binary.BigEndian.PutUint64(b[12:20], idx)
	return b
}

func (ds *DiskStorage) entryPrefix() []byte {
	b := make([]byte, 12)
	binary.BigEndian.PutUint64(b[0:8], ds.rid)
	binary.BigEndian.PutUint32(b[8:12], ds.gid)
	return b
}

func (ds *DiskStorage) parseIndex(key []byte) uint64 {
	AssertTrue(len(key) == 20)
	return binary.BigEndian.Uint64(key[12:20])
}

func (ds *DiskStorage) storeRaftID(id uint64) error {
	return ds.db.Update(func(txn *wisckey.Txn) error {
		var b [8]byte
		binary.BigEndian.PutUint64(b[:], id)
		return txn.Set(raftIDKey, b[:])
	})
}

func (ds *DiskStorage) seekEntry(e *pb.Entry, seekTo uint64, reverse bool) (index uint64, err error) {
	err = ds.db.View(func(txn *wisckey.Txn) error {
		opt := wisckey.DefaultIteratorOptions
		opt.PrefetchValues = false
		opt.Reverse = reverse
		itr := txn.NewIterator(opt)
		defer itr.Close()

		itr.Seek(ds.entryKey(seekTo))
		if !itr.ValidForPrefix(ds.entryPrefix()) {
			return errNotFound
		}
		item := itr.Item()
		index = ds.parseIndex(item.Key())
		if e == nil {
			return nil
		}
		return item.Value(func(val []byte) error {
			return e.Unmarshal(val)
		})
	})
	return index, err
}

func (ds *DiskStorage) allEntries(lo, hi, maxSize uint64) (es []pb.Entry, err error) {
	err = ds.db.View(func(txn *wisckey.Txn) error {
		if hi-lo == 1 { // We only need one entry.
			item, err := txn.Get(ds.entryKey(lo))
			if err != nil {
				return err
			}
			return item.Value(func(val []byte) error {
				var e pb.Entry
				if err = e.Unmarshal(val); err != nil {
					return err
				}
				es = append(es, e)
				return nil
			})
		}

		itr := txn.NewIterator(wisckey.DefaultIteratorOptions)
		defer itr.Close()

		start := ds.entryKey(lo)
		end := ds.entryKey(hi) // Not included in results.
		prefix := ds.entryPrefix()

		var size, lastIndex uint64
		first := true
		for itr.Seek(start); itr.ValidForPrefix(prefix); itr.Next() {
			item := itr.Item()
			var e pb.Entry
			if err := item.Value(func(val []byte) error {
				return e.Unmarshal(val)
			}); err != nil {
				return err
			}
			// If this Assert does not fail, then we can safely remove that strange append fix
			// below.
			AssertTrue(e.Index > lastIndex && e.Index >= lo)
			lastIndex = e.Index
			if bytes.Compare(item.Key(), end) >= 0 {
				break
			}
			size += uint64(e.Size())
			if size > maxSize && !first {
				break
			}
			es = append(es, e)
			first = false
		}
		return nil
	})
	return es, err
}

// reset resets the entries. Used for testing.
func (ds *DiskStorage) reset(es []pb.Entry) error {
	tu := ds.newTxnUnifier()
	defer tu.Cancel()

	if err := ds.deleteFrom(tu, 0); err != nil {
		return err
	}

	for _, e := range es {
		data, err := e.Marshal()
		if err != nil {
			return err
		}
		key := ds.entryKey(e.Index)
		if err := tu.Set(key, data); err != nil {
			return err
		}
	}

	return tu.Done()
}

// setSnapshot would store the snapshot. We can delete all the entries up until the snapshot
// index. But, keep the raft entry at the snapshot index, to make it easier to build the logic; like
// the dummy entry in MemoryStorage.
func (ds *DiskStorage) setSnapshot(u *txnUnifier, s pb.Snapshot) error {
	if IsEmptySnap(s) {
		return nil
	}

	data, err := s.Marshal()
	if err != nil {
		return err
	}
	if err := u.Set(ds.snapshotKey(), data); err != nil {
		return err
	}

	e := pb.Entry{Term: s.Metadata.Term, Index: s.Metadata.Index}
	data, err = e.Marshal()
	if err != nil {
		return err
	}
	if err := u.Set(ds.entryKey(e.Index), data); err != nil {
		return err
	}

	// Cache it.
	ds.cache.setSnapshot(s)
	return nil
}

// SetHardState saves the current HardState.
func (ds *DiskStorage) setHardState(u *txnUnifier, st pb.HardState) error {
	if IsEmptyHardState(st) {
		return nil
	}
	data, err := st.Marshal()
	if err != nil {
		return err
	}
	return u.Set(ds.hardStateKey(), data)
}

// delete entries in the range of index [from, inf).
func (ds *DiskStorage) deleteFrom(tu *txnUnifier, from uint64) error {
	var keys []string
	err := ds.db.View(func(txn *wisckey.Txn) error {
		start := ds.entryKey(from)
		prefix := ds.entryPrefix()

		opt := wisckey.DefaultIteratorOptions
		opt.PrefetchValues = false
		itr := txn.NewIterator(opt)
		defer itr.Close()

		for itr.Seek(start); itr.ValidForPrefix(prefix); itr.Next() {
			key := itr.Item().Key()
			keys = append(keys, string(key))
		}
		return nil
	})
	if err != nil {
		return err
	}
	return ds.deleteKeys(tu, keys)
}

// Delete all entries from [0, until), i.e. excluding until.
// Keep the entry at the snapshot index, for simplification of logic.
// It is the application's responsibility to not attempt to deleteUntil an index
// greater than raftLog.applied.
func (ds *DiskStorage) deleteUntil(u *txnUnifier, until uint64) error {
	var keys []string
	err := ds.db.View(func(txn *wisckey.Txn) error {
		opt := wisckey.DefaultIteratorOptions
		opt.PrefetchValues = false
		itr := txn.NewIterator(opt)
		defer itr.Close()

		start := ds.entryKey(0)
		prefix := ds.entryPrefix()
		first := true
		var index uint64
		for itr.Seek(start); itr.ValidForPrefix(prefix); itr.Next() {
			item := itr.Item()
			index = ds.parseIndex(item.Key())
			if first {
				first = false
				if until <= index {
					return ErrCompacted
				}
			}
			if index >= until {
				break
			}
			keys = append(keys, string(item.Key()))
		}
		return nil
	})
	if err != nil {
		return err
	}
	return ds.deleteKeys(u, keys)
}

func (ds *DiskStorage) deleteKeys(tu *txnUnifier, keys []string) error {
	if len(keys) == 0 {
		return nil
	}

	for _, k := range keys {
		if err := tu.Delete([]byte(k)); err != nil {
			return err
		}
	}

	return nil
}

// Append the new entries to storage.
// it will be part discard if entries.Index > firstIndex
// exist log entry will be turncated if lastIndex < entry.Index
// for example: raft log entry [5, 10]   appendEntries [4, 9]
// append result raft log entry [5, 9]
func (ds *DiskStorage) appendEntries(u *txnUnifier, entries []pb.Entry) error {
	if len(entries) == 0 {
		return nil
	}

	first, err := ds.FirstIndex()
	if err != nil {
		return err
	}
	firste := entries[0].Index
	if firste+uint64(len(entries))-1 < first {
		// All of these entries have already been compacted.
		return nil
	}
	if first > firste {
		// Truncate compacted entries
		entries = entries[first-firste:]
	}

	last, err := ds.LastIndex()
	if err != nil {
		return err
	}
	AssertTruef(firste <= last+1, "firste: %d. last: %d", firste, last)

	for _, e := range entries {
		key := ds.entryKey(e.Index)
		data, err := e.Marshal()
		if err != nil {
			return err
		}
		if err := u.Set(key, data); err != nil {
			return err
		}
	}

	laste := entries[len(entries)-1].Index
	if laste < last {
		return ds.deleteFrom(u, laste+1)
	}
	return nil
}
