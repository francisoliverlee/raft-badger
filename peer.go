package raft_badger

import (
	"encoding/json"
	"errors"
	"fmt"
	kvstore "github.com/gmqio/kv-store"
	"github.com/hashicorp/go-hclog"
	"github.com/hashicorp/raft"
	errors2 "github.com/pkg/errors"
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

	fsmStore kvstore.KvStore
}

func NewPeer(dir, bind string) *Peer {
	return &Peer{
		RaftDir:  dir,
		RaftBind: bind,
	}
}

// MakeSureLeader user set, delete should on leader. no need to read
func (s *Peer) MakeSureLeader() error {
	if s.raft.State() == raft.Leader {
		return nil
	} else {
		return errors.New("not leader")
	}
}

// Set a kv on leader
func (s *Peer) Set(bucket, k, v string) error {
	if e := s.MakeSureLeader(); e != nil {
		return e
	}

	c := NewFsmCommand(FsmCommandSet)
	c.Key = s.buildKey(bucket, k)
	c.Value = v

	b, err := json.Marshal(c)
	if err != nil {
		return err
	}

	f := s.raft.Apply(b, raftTimeout)
	if e1 := f.Error(); e1 != nil {
		return e1
	}

	var c2, ok = f.Response().(FsmCommand)
	if !ok {
		log.Printf("[Error] %s should got FsmCommand but not. Response: %v", c.Op, f.Response())
		return errors.New("fsm response type error")
	}
	return c2.Error
}

// Get a kv on any peer
func (s *Peer) Get(bucket, k string) (result string, found bool, e error) {
	bb := []byte(bucket)
	newBB := kvstore.AppendBytes(FsmBucketLength+len(bb), FsmBucket, bb)

	val, f, err := s.fsmStore.Get(newBB, []byte(k))
	return string(val), f, err
}

// PSet multi kv on leader, Non-atomic operation
func (s *Peer) PSet(bucket string, kv map[string]string) error {
	if e := s.MakeSureLeader(); e != nil {
		return e
	}

	for k, v := range kv {
		if err := s.Set(bucket, k, v); err != nil {
			return err
		}
	}

	return nil
}

// PGet multi keys' values on any peer
func (s *Peer) PGet(bucket string, keys []string) (map[string]string, error) {
	kb := [][]byte{}
	for _, k := range keys {
		kb = append(kb, []byte(k))
	}

	bb := []byte(bucket)
	newBB := kvstore.AppendBytes(FsmBucketLength+len(bb), FsmBucket, bb)

	vals, err := s.fsmStore.PGet(newBB, kb)
	if err != nil {
		return nil, err
	}

	m := make(map[string]string)
	for i, val := range vals {
		m[keys[i]] = string(val)
	}
	return m, nil
}

// Delete a kv on leader
func (s *Peer) Delete(bucket, key string) error {
	if e := s.MakeSureLeader(); e != nil {
		return e
	}

	c := NewFsmCommand(FsmCommandDel)
	c.Key = s.buildKey(bucket, key)

	b, err := json.Marshal(c)
	if err != nil {
		return err
	}

	f := s.raft.Apply(b, raftTimeout)

	if e1 := f.Error(); e1 != nil {
		return e1
	}

	var c2, ok = f.Response().(FsmCommand)
	if !ok {
		log.Printf("[Error] %s should got FsmCommand but not. Response: %v", c.Op, f.Response())
		return errors.New("fsm response type error")
	}
	return c2.Error
}

// PDelete multi kv on leader, Non-atomic operation
func (s *Peer) PDelete(bucket string, keys []string) error {
	for _, key := range keys {
		if err := s.Delete(bucket, key); err != nil {
			return err
		}
	}
	return nil
}

// Keys all in bucket
func (s *Peer) Keys(bucket string) (map[string]string, error) {
	bb := []byte(bucket)
	nbb := kvstore.AppendBytes(FsmBucketLength+len(bb), FsmBucket, bb)

	keys, vals, err := s.fsmStore.Keys(nbb)
	if err != nil {
		return nil, errors2.Wrap(err, fmt.Sprintf("failed to %s in bucket: %s", bucket))
	}

	if keys == nil || len(keys) == 0 || vals == nil || len(vals) == 0 {
		log.Printf("empty in bucket " + bucket)
		return map[string]string{}, nil
	}

	if len(keys) != len(vals) {
		str := fmt.Sprintf("[Error] length of keys[%d] and values[%d] not the same in bucket %s", len(keys), len(vals), bucket)
		return map[string]string{}, errors.New(str)
	}

	m := map[string]string{}
	for i, key := range keys {
		m[string(key)] = string(vals[i])
	}
	return m, nil
}

// KeysWithoutValues in bucket
func (s *Peer) KeysWithoutValues(bucket string) (keys []string, err error) {
	m, e := s.Keys(bucket)
	return MKeys(m), e
}

// Close peer
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

	// Local fsm store
	s.fsmStore = badgerStore.db

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
	if len(bucket) == 0 {
		return key
	}
	return bucket + BucketSpliter + key
}
