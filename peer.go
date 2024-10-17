package raft_badger

import (
	"encoding/json"
	"errors"
	"fmt"
	"github.com/hashicorp/go-hclog"
	"github.com/hashicorp/raft"
	"log"
	"net"
	"os"
	"path/filepath"
	"time"
)

const (
	retainSnapshotCount = 2
	raftTimeout         = 10 * time.Second
)

type Peer struct {
	RaftDir  string
	RaftBind string

	raft *raft.Raft // The consensus mechanism

	logger *log.Logger
}

func NewPeer() *Peer {
	return &Peer{
		logger: log.New(os.Stderr, "[Peer] ", log.LstdFlags),
	}
}

func (s *Peer) Open(enableSingle bool, localID string) error {
	config := raft.DefaultConfig()
	config.LocalID = raft.ServerID(localID)

	// Setup Raft communication.
	addr, err := net.ResolveTCPAddr("tcp", s.RaftBind)
	if err != nil {
		return err
	}
	transport, err := raft.NewTCPTransport(s.RaftBind, addr, 3, 10*time.Second, os.Stderr)
	if err != nil {
		return err
	}

	// Create the snapshot store. This allows the Raft to truncate the log.
	snapshots, err := raft.NewFileSnapshotStoreWithLogger(s.RaftDir, retainSnapshotCount, hclog.New(&hclog.LoggerOptions{
		Name:   "raft.snapshot",
		Output: os.Stderr,
		Level:  hclog.DefaultLevel,
	}))
	if err != nil {
		return fmt.Errorf("file snapshot store: %s", err)
	}

	badgerStore, err := NewBadgerStore(filepath.Join(s.RaftDir, "raft.log"))
	if err != nil {
		return fmt.Errorf("new badger store: %s", err)
	}

	// Instantiate the Raft systems.
	ra, err := raft.NewRaft(config, badgerStore, badgerStore, badgerStore, snapshots, transport)
	if err != nil {
		return fmt.Errorf("new raft: %s", err)
	}
	s.raft = ra

	if enableSingle {
		configuration := raft.Configuration{
			Servers: []raft.Server{
				{
					ID:      config.LocalID,
					Address: transport.LocalAddr(),
				},
			},
		}
		ra.BootstrapCluster(configuration)
	}

	return nil
}

func (s *Peer) Join(nodeID, addr string) error {
	s.logger.Printf("received join request for remote node %s at %s", nodeID, addr)

	configFuture := s.raft.GetConfiguration()
	if err := configFuture.Error(); err != nil {
		s.logger.Printf("failed to get raft configuration: %v", err)
		return err
	}

	for _, srv := range configFuture.Configuration().Servers {
		// If a node already exists with either the joining node's ID or address,
		// that node may need to be removed from the config first.
		if srv.ID == raft.ServerID(nodeID) || srv.Address == raft.ServerAddress(addr) {
			// However if *both* the ID and the address are the same, then nothing -- not even
			// a join operation -- is needed.
			if srv.Address == raft.ServerAddress(addr) && srv.ID == raft.ServerID(nodeID) {
				s.logger.Printf("node %s at %s already member of cluster, ignoring join request", nodeID, addr)
				return nil
			}

			future := s.raft.RemoveServer(srv.ID, 0, 0)
			if err := future.Error(); err != nil {
				return fmt.Errorf("error removing existing node %s at %s: %s", nodeID, addr, err)
			}
		}
	}

	f := s.raft.AddVoter(raft.ServerID(nodeID), raft.ServerAddress(addr), 0, 0)
	if f.Error() != nil {
		return f.Error()
	}
	s.logger.Printf("node %s at %s joined successfully", nodeID, addr)
	return nil
}

func (s *Peer) Get(key string) (string, error) {
	c := &FsmCommand{
		Op:  FsmCommandGet,
		Key: key,
	}

	b, err := json.Marshal(c)
	if err != nil {
		return "", err
	}

	f := s.raft.Apply(b, raftTimeout)

	if f.Error() != nil {
		return "", f.Error()
	}

	if c2, ok := f.Response().(FsmCommand); ok {
		if c2.Error != nil {
			return "", c2.Error
		}
		return c2.Value, nil
	} else {
		log.Fatalf("[BUG] should got FsmCommand, but not.Response: %v", f.Response())
		return "", errors.New("invalid response")
	}
}

func (s *Peer) Set(key, value string) error {
	if s.raft.State() != raft.Leader {
		return fmt.Errorf("not leader")
	}

	c := &FsmCommand{
		Op:    FsmCommandSet,
		Key:   key,
		Value: value,
	}
	b, err := json.Marshal(c)
	if err != nil {
		return err
	}

	f := s.raft.Apply(b, raftTimeout)
	return f.Error()
}

func (s *Peer) Delete(key string) error {
	if s.raft.State() != raft.Leader {
		return fmt.Errorf("not leader")
	}

	c := &FsmCommand{
		Op:  FsmCommandDelete,
		Key: key,
	}
	b, err := json.Marshal(c)
	if err != nil {
		return err
	}

	f := s.raft.Apply(b, raftTimeout)
	return f.Error()
}

func (s *Peer) Stats() map[string]string {
	return s.raft.Stats()
}
