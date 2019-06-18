package network

import (
	"context"
	"sync"

	pb "github.com/shimingyah/raft/raftpb"
)

// Server raft service
type Server struct {
	Node *Node
	sync.RWMutex
}

// Ping see RaftServer interface.
func (s *Server) Ping(ctx context.Context, payload *pb.Payload) (*pb.Payload, error) {
	return &pb.Payload{Data: payload.Data}, ctx.Err()
}

// Send see RaftServer interface.
func (s *Server) Send(ctx context.Context, batch *pb.Batch) (*pb.Payload, error) {
	return &pb.Payload{}, nil
}

// Join see RaftServer interface.
func (s *Server) Join(ctx context.Context, rc *pb.Context) (*pb.Payload, error) {
	return &pb.Payload{}, nil
}

// Peers see RaftServer interface.
func (s *Server) Peers(ctx context.Context, rc *pb.Context) (*pb.Payload, error) {
	return &pb.Payload{}, nil
}
