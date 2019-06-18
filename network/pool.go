package network

import (
	"bytes"
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/shimingyah/raft"
	pb "github.com/shimingyah/raft/raftpb"
	"google.golang.org/grpc"
)

var (
	ErrNoConnection        = fmt.Errorf("No connection exists")
	ErrUnhealthyConnection = fmt.Errorf("Unhealthy connection")
	errNoPeerPoolEntry     = fmt.Errorf("no peerPool entry")
	errNoPeerPool          = fmt.Errorf("no peerPool pool, could not connect")
	echoDuration           = 10 * time.Second
)

// GrpcMaxSize Use the max possible grpc msg size for the most flexibility (4GB - equal
// to the max grpc frame size). Users will still need to set the max
// message sizes allowable on the client size when dialing.
const GrpcMaxSize = 4 << 30

// Pool is used to manage the grpc client connection(s) for communicating with other
// worker instances.  Right now it just holds one of them.
// A "pool" now consists of one connection.  gRPC uses HTTP2 transport to combine
// messages in the same TCP stream.
type Pool struct {
	Addr     string
	conn     *grpc.ClientConn
	lastEcho time.Time
	ticker   *time.Ticker
	sync.RWMutex
}

// Pools is used to manage multi grpc connections
type Pools struct {
	all map[string]*Pool
	sync.RWMutex
}

var pi *Pools

func init() {
	pi = new(Pools)
	pi.all = make(map[string]*Pool)
}

// Get return pools
func Get() *Pools {
	return pi
}

// Get return the given address connection
func (p *Pools) Get(addr string) (*Pool, error) {
	p.RLock()
	defer p.RUnlock()
	pool, ok := p.all[addr]
	if !ok {
		return nil, ErrNoConnection
	}
	if !pool.IsHealthy() {
		return nil, ErrUnhealthyConnection
	}
	return pool, nil
}

// Remove the given address connection
func (p *Pools) Remove(addr string) {
	p.Lock()
	pool, ok := p.all[addr]
	if !ok {
		p.Unlock()
		return
	}
	delete(p.all, addr)
	p.Unlock()
	pool.shutdown()
}

// Connect get or new connection
func (p *Pools) Connect(addr string) *Pool {
	p.RLock()
	existingPool, has := p.all[addr]
	if has {
		p.RUnlock()
		return existingPool
	}
	p.RUnlock()

	pool, err := NewPool(addr)
	if err != nil {
		raft.GetLogger().Errorf("Unable to connect to host: %s", addr)
		return nil
	}

	p.Lock()
	existingPool, has = p.all[addr]
	if has {
		p.Unlock()
		return existingPool
	}
	raft.GetLogger().Infof("== CONNECTED ==> Setting %v\n", addr)
	p.all[addr] = pool
	p.Unlock()

	return pool
}

// NewPool creates a new "pool" with one gRPC connection, refcount 0.
func NewPool(addr string) (*Pool, error) {
	conn, err := grpc.Dial(addr,
		grpc.WithDefaultCallOptions(
			grpc.MaxCallRecvMsgSize(GrpcMaxSize),
			grpc.MaxCallSendMsgSize(GrpcMaxSize)),
		grpc.WithBackoffMaxDelay(time.Second),
		grpc.WithInsecure())
	if err != nil {
		return nil, err
	}
	pl := &Pool{conn: conn, Addr: addr, lastEcho: time.Now()}
	pl.CheckHealth()

	// Initialize ticker before running monitor health.
	pl.ticker = time.NewTicker(echoDuration)
	go pl.MonitorHealth()
	return pl, nil
}

// Get returns the connection to use from the pool of connections.
func (p *Pool) Get() *grpc.ClientConn {
	p.RLock()
	defer p.RUnlock()
	return p.conn
}

func (p *Pool) shutdown() {
	p.ticker.Stop()
	p.conn.Close()
}

// CheckHealth check connection if is alive.
func (p *Pool) CheckHealth() error {
	conn := p.Get()
	query := &pb.Payload{
		Data: make([]byte, 11),
	}
	copy(query.Data, []byte("hello world"))

	client := pb.NewRaftClient(conn)
	resp, err := client.Ping(context.Background(), query)
	if err == nil {
		if bytes.Equal(resp.Data, query.Data) {
			p.Lock()
			p.lastEcho = time.Now()
			p.Unlock()
		}
	} else {
		raft.GetLogger().Errorf("Ping error from %v. Err: %v\n", p.Addr, err)
	}

	return err
}

// MonitorHealth monitors the health of the connection via Echo. This function blocks forever.
func (p *Pool) MonitorHealth() {
	var lastErr error
	for range p.ticker.C {
		err := p.CheckHealth()
		if lastErr != nil && err == nil {
			raft.GetLogger().Infof("Connection established with %v\n", p.Addr)
		}
		lastErr = err
	}
}

// IsHealthy check connection if is alive
func (p *Pool) IsHealthy() bool {
	p.RLock()
	defer p.RUnlock()
	return time.Since(p.lastEcho) < 2*echoDuration
}
