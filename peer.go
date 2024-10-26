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

	logStoreDir      = "raft.log"
	snapshotStoreDir = "raft.snapshot"
)

type Peer struct {
	Id       string
	RaftDir  string
	RaftBind string

	raft             *raft.Raft // The consensus mechanism
	hasExistingState bool

	snpss *raft.FileSnapshotStore
	ls    *logStore
	fs    *fsmStore
	ss    *stableStore
}

func NewPeer(id, dir, bind string) *Peer {
	return &Peer{
		Id:       id,
		RaftDir:  dir,
		RaftBind: bind,
	}
}

/*
Open a peer, ready to do join or joined by other peers
@localID peer id
@exist is useful when @err is nil
*/
func (p *Peer) Open() (err error) {
	config := raft.DefaultConfig()
	config.LocalID = raft.ServerID(p.Id)

	// Setup Raft communication.
	addr, err := net.ResolveTCPAddr("tcp", p.RaftBind)
	if err != nil {
		return err
	}

	// Create raft transportation for peer-to-peer-call for raft state change
	transport, err := raft.NewTCPTransport(p.RaftBind, addr, 3, 10*time.Second, os.Stderr)
	if err != nil {
		return err
	}

	// Create the snapshot store. This allows the Raft to truncate the log.
	if snpss, err := raft.NewFileSnapshotStoreWithLogger(p.RaftDir, retainSnapshotCount, hclog.New(&hclog.LoggerOptions{
		Name:   snapshotStoreDir,
		Output: os.Stderr,
		Level:  hclog.DefaultLevel,
	})); err != nil {
		return fmt.Errorf("file snapshot store: %s", err)
	} else {
		p.snpss = snpss
	}

	// Create raft log store
	if logStore, err := newLogStore(filepath.Join(p.RaftDir, logStoreDir), false); err != nil {
		return fmt.Errorf("new badger store: %s", err)
	} else {
		p.ls = logStore
	}
	// Create local fsmStore store
	p.fs = newFsm(p.ls.db)

	// Create raft stable store
	p.ss = newStableStore(p.ls.db)

	ra, err := raft.NewRaft(config, p.fs, p.ls, p.ss, p.snpss, transport)
	if err != nil {
		return fmt.Errorf("new raft: %s", err)
	}

	p.raft = ra
	if existingState, e := raft.HasExistingState(p.ls, p.ss, p.snpss); e != nil {
		return err
	} else {
		p.hasExistingState = existingState
	}
	return nil
}

// StartSingle raft peer
func (p *Peer) StartSingle() error {
	log.Printf("BootstrapCluster single as leader peer, id=%s, address=%s, hasExistingState=%v", p.Id, p.RaftBind, p.hasExistingState)
	configuration := raft.Configuration{
		Servers: []raft.Server{
			{
				ID:      raft.ServerID(p.Id),
				Address: raft.ServerAddress(p.RaftBind),
			},
		},
	}
	return p.raft.BootstrapCluster(configuration).Error()
}

// Join a raft cluster
func (p *Peer) Join(peerId, addr string) error {
	log.Printf("received join request for remote node %s at %s", peerId, addr)
	if p.raft.State() != raft.Leader {
		return errors.New(fmt.Sprintf("join should run on leader. peer id=%s, address=%s", peerId, addr))
	}
	configFuture := p.raft.GetConfiguration()
	if err := configFuture.Error(); err != nil {
		log.Printf("failed to get raft configuration: %v", err)
		return err
	}

	if err := p.Remove(peerId, addr); err != nil {
		return err
	}

	f := p.raft.AddVoter(raft.ServerID(peerId), raft.ServerAddress(addr), 0, 0)
	if f.Error() != nil {
		return f.Error()
	}

	log.Printf("node %s at %s joined successfully", peerId, addr)
	return nil
}

// Remove a peer
func (p *Peer) Remove(peerId, addr string) error {
	log.Printf("received Remove request for remote node %s at %s", peerId, addr)
	if p.raft.State() != raft.Leader {
		return errors.New(fmt.Sprintf("leave should run on leader. peer id=%s, address=%s", peerId, addr))
	}

	for _, srv := range p.raft.GetConfiguration().Configuration().Servers {
		// If a node already exists with either the joining node'p ID or address,
		// that node may need to be removed from the config first.
		if srv.ID == raft.ServerID(peerId) || srv.Address == raft.ServerAddress(addr) {
			// However if *both* the ID and the address are the same, then nothing -- not even
			// a join operation -- is needed.
			if srv.Address == raft.ServerAddress(addr) && srv.ID == raft.ServerID(peerId) {
				log.Printf("node %s at %s already member of cluster, ignoring join request", peerId, addr)
				return nil
			}

			future := p.raft.RemoveServer(srv.ID, 0, 0)
			idx := future.Index()
			err := future.Error()

			log.Printf("response Remove request for remote node %s at %s, idx %d, err %v", peerId, addr, idx, err)
			if err != nil {
				return fmt.Errorf("error removing existing node %s at %s: %s", peerId, addr, err)
			}
		}
	}

	return nil
}

// MakeSureLeader user set, delete should on leader. no need to read
func (p *Peer) MakeSureLeader() error {
	if p.raft.State() == raft.Leader {
		return nil
	} else {
		return errors.New("not leader")
	}
}

// Set a kv on leader
func (p *Peer) Set(bucket, k, v string) error {
	if e := p.MakeSureLeader(); e != nil {
		return e
	}

	c := newFsmCommand(FsmCommandSet)
	c.Kv[p.buildKey(bucket, k)] = v

	b, err := json.Marshal(c)
	if err != nil {
		return err
	}

	f := p.raft.Apply(b, raftTimeout)
	if e1 := f.Error(); e1 != nil {
		return e1
	}

	var c2, ok = f.Response().(FsmCommand)
	if !ok {
		log.Printf("[Error] %s should got FsmCommand but not. Response: %v", c.Op, f.Response())
		return errors.New("fsmStore response type error")
	}
	return c2.Error
}

// Get a kv on any peer
func (p *Peer) Get(bucket, k string) (result string, found bool, e error) {
	newKey := p.buildKey(bucket, k)
	val, f, err := p.fs.get([]byte(newKey))
	return string(val), f, err
}

// PSet multi kv on leader, Non-atomic operation
func (p *Peer) PSet(bucket string, kv map[string]string) error {
	if e := p.MakeSureLeader(); e != nil {
		return e
	}

	for k, v := range kv {
		if err := p.Set(bucket, k, v); err != nil {
			return err
		}
	}

	return nil
}

// PGet multi keys' values on any peer
func (p *Peer) PGet(bucket string, keys []string) (map[string]string, error) {
	kba := [][]byte{}
	for _, k := range keys {
		kba = append(kba, []byte(p.buildKey(bucket, k)))
	}

	vals, err := p.fs.pget(kba)
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
func (p *Peer) Delete(bucket, key string) error {
	if e := p.MakeSureLeader(); e != nil {
		return e
	}

	c := newFsmCommand(FsmCommandDel)
	c.Kv[p.buildKey(bucket, key)] = ""

	b, err := json.Marshal(c)
	if err != nil {
		return err
	}

	f := p.raft.Apply(b, raftTimeout)

	if e1 := f.Error(); e1 != nil {
		return e1
	}

	var c2, ok = f.Response().(FsmCommand)
	if !ok {
		log.Printf("[Error] %s should got FsmCommand but not. Response: %v", c.Op, f.Response())
		return errors.New("fsmStore response type error")
	}
	return c2.Error
}

// PDelete multi kv on leader, Non-atomic operation
func (p *Peer) PDelete(bucket string, keys []string) error {
	for _, key := range keys {
		if err := p.Delete(bucket, key); err != nil {
			return err
		}
	}
	return nil
}

// Keys all in bucket
func (p *Peer) Keys(bucket string) (map[string]string, error) {
	bb := []byte(bucket)
	nbb := kvstore.AppendBytes(FsmBucketLength+len(bb), FsmBucket, bb)

	keys, vals, err := p.fs.keys(nbb)
	if err != nil {
		return nil, errors2.Wrap(err, fmt.Sprintf("failed to keys in bucket: %s", bucket))
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
func (p *Peer) KeysWithoutValues(bucket string) (keys []string, err error) {
	m, e := p.Keys(bucket)
	return MKeys(m), e
}

// Close peer
func (p *Peer) Close() error {
	if err := p.raft.Shutdown().Error(); err != nil {
		return err
	}
	if err := p.ls.Close(); err != nil {
		return err
	}
	return nil
}

/*
applied_index:2
commit_index:2
fsm_pending:0
last_contact:0
last_log_index:2
last_log_term:2
last_snapshot_index:0
last_snapshot_term:0
latest_configuration:[{Suffrage:Voter
ID:node0
Address:127.0.0.1:0}]
latest_configuration_index:0
num_peers:0
protocol_version:3
protocol_version_max:3
protocol_version_min:0
snapshot_version_max:1
snapshot_version_min:0
state:Leader
term:2
*/
func (p *Peer) Stats() map[string]string {
	return p.raft.Stats()
}

func (p *Peer) buildKey(bucket, key string) string {
	if len(bucket) == 0 {
		return key
	}
	return bucket + BucketSpliter + key
}
