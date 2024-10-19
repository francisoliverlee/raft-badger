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
	BucketSpliter       = "@"
	retainSnapshotCount = 2
	raftTimeout         = 10 * time.Second
)

type Peer struct {
	RaftDir  string
	RaftBind string

	raft *raft.Raft // The consensus mechanism
}

func NewPeer() *Peer {
	return &Peer{}
}

func (s *Peer) MakeSureLeader() error {
	if s.raft.State() == raft.Leader {
		return nil
	} else {
		return errors.New("not leader")
	}
}

func (s *Peer) Set(bucket, k, v string) error {
	if e := s.MakeSureLeader(); e != nil {
		return e
	}

	c := NewFsmCommand(FsmCommandSet)
	c.Kv[s.buildKey(bucket, k)] = v

	b, err := json.Marshal(c)
	if err != nil {
		return err
	}

	f := s.raft.Apply(b, raftTimeout)
	return f.Error()
}

func (s *Peer) Get(bucket, k string) (result string, found bool, e error) {
	if e := s.MakeSureLeader(); e != nil {
		return "", false, e
	}

	c := NewFsmCommand(FsmCommandGet)
	newKey := s.buildKey(bucket, k)
	c.Kv[newKey] = ""

	b, err := json.Marshal(c)
	if err != nil {
		return "", false, err
	}

	f := s.raft.Apply(b, raftTimeout)
	if c2, ok := f.Response().(FsmCommand); ok {
		if c2.Error != nil {
			return "", false, c2.Error
		}
		result, found := c2.Kv[newKey]
		return result, found, nil
	} else {
		log.Fatalf("[BUG PGet] should got FsmCommand, but not.Response: %v", f.Response())
		return "", false, errors.New("invalid response")
	}
}

func (s *Peer) PSet(bucket string, kv map[string]string) error {
	if e := s.MakeSureLeader(); e != nil {
		return e
	}

	c := NewFsmCommand(FsmCommandPSet)
	for k, v := range kv {
		c.Kv[s.buildKey(bucket, k)] = v
	}

	b, err := json.Marshal(c)
	if err != nil {
		return err
	}

	f := s.raft.Apply(b, raftTimeout)
	return f.Error()
}

func (s *Peer) PGet(bucket string, keys []string) (map[string]string, error) {
	newKeys := make([]string, len(keys))
	for _, key := range keys {
		newKeys = append(newKeys, s.buildKey(bucket, key))
	}

	c := NewFsmCommand(FsmCommandPGet)

	b, err := json.Marshal(c)
	if err != nil {
		return nil, err
	}

	f := s.raft.Apply(b, raftTimeout)

	if f.Error() != nil {
		return nil, f.Error()
	}

	if c2, ok := f.Response().(FsmCommand); ok {
		if c2.Error != nil {
			return nil, c2.Error
		}
		return c2.Kv, nil
	}
	// never reach here
	return nil, nil
}

func (s *Peer) Delete(bucket, key string) error {
	if e := s.MakeSureLeader(); e != nil {
		return e
	}

	c := NewFsmCommand(FsmCommandDel)
	newKey := s.buildKey(bucket, key)
	c.Kv[newKey] = ""

	b, err := json.Marshal(c)
	if err != nil {
		return err
	}

	f := s.raft.Apply(b, raftTimeout)

	if f.Error() != nil {
		return f.Error()
	}

	if c2, ok := f.Response().(FsmCommand); ok {
		if c2.Error != nil {
			return c2.Error
		}
		return nil
	}
	// never reach here
	return nil
}

func (s *Peer) PDel(bucket string, keys []string) error {
	if e := s.MakeSureLeader(); e != nil {
		return e
	}

	c := NewFsmCommand(FsmCommandPDel)
	for _, key := range keys {
		c.Kv[s.buildKey(bucket, key)] = ""
	}
	b, err := json.Marshal(c)
	if err != nil {
		return err
	}

	f := s.raft.Apply(b, raftTimeout)

	if f.Error() != nil {
		return f.Error()
	}

	if c2, ok := f.Response().(FsmCommand); ok {
		if c2.Error != nil {
			return c2.Error
		}
		return nil
	}

	// never reach here
	return nil
}

func (s *Peer) Keys(bucket string) (map[string]string, error) {
	c := NewFsmCommand(FsmCommandKeys)
	c.Bucket = bucket

	b, err := json.Marshal(c)
	if err != nil {
		return nil, err
	}

	f := s.raft.Apply(b, raftTimeout)

	if f.Error() != nil {
		return nil, f.Error()
	}

	if c2, ok := f.Response().(FsmCommand); ok {
		if c2.Error != nil {
			return nil, c2.Error
		}
		return c2.Kv, nil
	}
	// never reach here
	return nil, nil
}

func (s *Peer) KeysWithoutValues(bucket string) (keys []string, err error) {
	m, e := s.Keys(bucket)
	return MKeys(m), e
}

func (s *Peer) Close() error {
	return s.raft.Shutdown().Error()
}

// Open a peer, ready to do join or joined by other peers
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

	badgerStore, err := NewBadgerStore(filepath.Join(s.RaftDir, "raft.log"), false)
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

// Join a raft cluster
func (s *Peer) Join(nodeID, addr string) error {
	log.Printf("received join request for remote node %s at %s", nodeID, addr)

	configFuture := s.raft.GetConfiguration()
	if err := configFuture.Error(); err != nil {
		log.Printf("failed to get raft configuration: %v", err)
		return err
	}

	for _, srv := range configFuture.Configuration().Servers {
		// If a node already exists with either the joining node's ID or address,
		// that node may need to be removed from the config first.
		if srv.ID == raft.ServerID(nodeID) || srv.Address == raft.ServerAddress(addr) {
			// However if *both* the ID and the address are the same, then nothing -- not even
			// a join operation -- is needed.
			if srv.Address == raft.ServerAddress(addr) && srv.ID == raft.ServerID(nodeID) {
				log.Printf("node %s at %s already member of cluster, ignoring join request", nodeID, addr)
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
	log.Printf("node %s at %s joined successfully", nodeID, addr)
	return nil
}

func (s *Peer) Stats() map[string]string {
	return s.raft.Stats()
}

func (s *Peer) buildKey(bucket, key string) string {
	return bucket + BucketSpliter + key
}
