package raft_badger

import (
	"encoding/json"
	"errors"
	"fmt"
	"github.com/dgraph-io/badger/v4"
	kvstore "github.com/gmqio/kv-store"
	"github.com/hashicorp/raft"
	errors2 "github.com/pkg/errors"
	"io"
	"log"
)

const (
	FsmCommandSet = "Set"
	FsmCommandDel = "Delete"
)

var (
	FsmBucket       = []byte("raft_fsm_") // raft fsmStore bucket
	FsmBucketLength = len(FsmBucket)

	cmd = map[string]interface{}{
		FsmCommandSet: nil,
		FsmCommandDel: nil,
	}
)

type fsmStore struct {
	db kvstore.KvStore
}

type FsmCommand struct {
	Op    string `json:"op,omitempty"`
	Error error  `json:"-"` // only output, not input param,

	Bucket string            `json:"bucket,omitempty"` // check where to be used
	Kv     map[string]string `json:"kv,omitempty"`
}

type fsmSnapshot struct {
	store map[string]string
}

// based on a kv store
func newFsm(db kvstore.KvStore) *fsmStore {
	return &fsmStore{
		db: db,
	}
}

func newFsmCommand(op string) FsmCommand {
	return FsmCommand{
		Op: op,
		Kv: map[string]string{},
	}
}

func newFsmSnapshot() *fsmSnapshot {
	return &fsmSnapshot{
		store: map[string]string{},
	}
}

// Persist should dump all necessary state to the WriteCloser 'sink',
// and call sink.Close() when finished or call sink.Cancel() on error.
func (f *fsmSnapshot) Persist(sink raft.SnapshotSink) error {
	err := func() error {
		// WARN : Big f.store would cost lots of time
		b, err := json.Marshal(f.store)
		if err != nil {
			return err
		}

		// Write data to sink.
		if _, err := sink.Write(b); err != nil {
			return err
		}

		// Close the sink.
		return sink.Close()
	}()

	if err != nil {
		return sink.Cancel()
	}

	return err
}

// Release is invoked when we are finished with the snapshot.
func (f *fsmSnapshot) Release() {
	f.store = map[string]string{}
}

// ok to check op
func (c FsmCommand) ok() bool {
	_, ok := cmd[c.Op]
	return ok
}

// Apply a log to user's local fsmStore after it's commited
func (b *fsmStore) Apply(l *raft.Log) interface{} {
	kvstore.L("Apply", []byte(l.Type.String()), l.Data)
	var c = newFsmCommand("")
	if err := json.Unmarshal(l.Data, &c); err != nil {
		c.Error = errors2.Wrap(err, fmt.Sprintf("failed to apply. unmarshal log error, index: %d", l.Index))
		return c
	}
	if !c.ok() {
		c.Error = errors.New(fmt.Sprintf("failed to apply, FsmCmd op error,op=%s, index=%d", c.Op, l.Index))
		return c
	}
	if c.Kv == nil || len(c.Kv) == 0 {
		log.Printf("[WARN] FsmCommand with empty kv. data=%s, index=%d, type=%s, t=%s", string(l.Data), l.Index, l.Type.String(), l.AppendedAt.String())
		return c
	}
	var keys [][]byte
	var values [][]byte

	for k, v := range c.Kv {
		keys = append(keys, []byte(k))
		switch c.Op {
		case FsmCommandSet:
			values = append(values, []byte(v))
		}
	}
	switch c.Op {
	case FsmCommandSet:
		if err := b.db.PSet(FsmBucket, keys, values); err != nil {
			c.Error = errors2.Wrap(err, fmt.Sprintf("failed to apply %s, bucket: %s", c.Op, c.Bucket))
		}
	case FsmCommandDel:
		if err := b.db.DeleteKeys(FsmBucket, keys); err != nil {
			c.Error = errors2.Wrap(err, fmt.Sprintf("failed to apply %s, bucket: %s", c.Op, c.Bucket))
		}
	}
	return c
}

// Snapshot The Snapshot implementation should return quickly, because Apply can not
// be called while Snapshot is running
func (b *fsmStore) Snapshot() (raft.FSMSnapshot, error) {
	kvMap := map[string]string{}

	if keys, vals, err := b.db.KeyStrings(FsmBucket, FsmBucket); err != nil {
		return &fsmSnapshot{store: kvMap}, err
	} else {
		for i, k := range keys {
			kvMap[k] = string(vals[i])
		}
	}

	return &fsmSnapshot{store: kvMap}, nil
}

// Restore is used to restore an FSM from a snapshot. It is not called
// concurrently with any other command. The FSM must discard all previous
// state before restoring the snapshot.
func (b *fsmStore) Restore(rc io.ReadCloser) error {
	newKv := make(map[string]string)
	if err := json.NewDecoder(rc).Decode(&newKv); err != nil {
		return err
	}

	s, sEr := b.Snapshot()
	if sEr != nil {
		log.Printf("Restore warn, get snapshot error. %s.  would replace all kv", sEr)
	}
	var fsmS = s.(*fsmSnapshot)
	PrintMapDiff(fsmS.store, newKv)

	// NOTICE : big newKv would spend lots of time
	return b.db.Exec(func(txn *badger.Txn) error {
		// clean all state

		for k, v := range newKv {
			kb := []byte(k)
			key := kvstore.BuildKey(len(FsmBucket)+len(kb), FsmBucket, kb)
			if err := txn.Set(key, []byte(v)); err != nil {
				return err
			}
		}
		return nil
	})
}

// ApplyBatch not an atomic operation
func (b *fsmStore) ApplyBatch(logs []*raft.Log) []interface{} {
	var res []interface{}
	for _, l := range logs {
		res = append(res, b.Apply(l))
	}
	return res
}

// get from fsmStore
func (b *fsmStore) get(k []byte) (result []byte, found bool, e error) {
	return b.db.Get(FsmBucket, k)
}

// pget from fsmStore
func (b *fsmStore) pget(keys [][]byte) ([][]byte, error) {
	return b.db.PGet(FsmBucket, keys)
}

// keys all in fsmStore
func (b *fsmStore) keys(userBucket []byte) (keys [][]byte, values [][]byte, err error) {
	prefix := kvstore.BuildKey(FsmBucketLength+len(userBucket), FsmBucket, userBucket)
	return b.db.Keys(FsmBucket, prefix)
}

// keys all in fsmStore
func (b *fsmStore) keyStrings(userBucket []byte) (keys []string, values [][]byte, err error) {
	prefix := kvstore.BuildKey(FsmBucketLength+len(userBucket), FsmBucket, userBucket)
	return b.db.KeyStrings(FsmBucket, prefix)
}

// keys all in fsmStore without return values
func (b *fsmStore) keysWithoutValues(userBucket []byte) (keys [][]byte, err error) {
	prefix := kvstore.BuildKey(FsmBucketLength+len(userBucket), FsmBucket, userBucket)
	return b.db.KeysWithoutValues(FsmBucket, prefix)
}

// keys all in fsmStore without return values
func (b *fsmStore) keyStringsWithoutValues(userBucket []byte) (keys []string, err error) {
	prefix := kvstore.BuildKey(FsmBucketLength+len(userBucket), FsmBucket, userBucket)
	return b.db.KeyStringsWithoutValues(FsmBucket, prefix)
}

func (p *fsmStore) Close() error {
	return p.db.Close()
}
