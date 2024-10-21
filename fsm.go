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

type fsm struct {
	db kvstore.KvStore
}

type FsmCommand struct {
	Op    string `json:"op,omitempty"`
	Error error  `json:"-"` // only output, not input param,

	Bucket string            `json:"bucket,omitempty"` // check where to be used
	Kv     map[string]string `json:"kv,omitempty"`
}

// based on a kv store
func newFsm(db kvstore.KvStore) *fsm {
	return &fsm{
		db: db,
	}
}

func newFsmCommand(op string) FsmCommand {
	return FsmCommand{
		Op: op,
		Kv: map[string]string{},
	}
}

type fsmSnapshot struct {
	store map[string]string
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
	_, ok := FsmCmd[c.Op]
	return ok
}

// Apply a log to user's local fsm after it's commited
func (b *fsm) Apply(l *raft.Log) interface{} {
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
			c.Error = errors2.Wrap(err, fmt.Sprintf("failed to apply %s, bucket: %s, key=%s", c.Op, c.Bucket))
		}
	case FsmCommandDel:
		if err := b.db.DeleteKeys(FsmBucket, keys); err != nil {
			c.Error = errors2.Wrap(err, fmt.Sprintf("failed to apply %s, bucket: %s, key=%s", c.Op, c.Bucket))
		}
	}
	return c
}

// Snapshot The Snapshot implementation should return quickly, because Apply can not
// be called while Snapshot is running
func (b *fsm) Snapshot() (raft.FSMSnapshot, error) {
	kvMap := map[string]string{}

	if keys, vals, err := b.db.KeyStrings(FsmBucket); err != nil {
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
func (b *fsm) Restore(rc io.ReadCloser) error {
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
			key := kvstore.AppendBytes(len(FsmBucket)+len(kb), FsmBucket, kb)
			if err := txn.Set(key, []byte(v)); err != nil {
				return err
			}
		}
		return nil
	})
}

// ApplyBatch not an atomic operation
func (b *fsm) ApplyBatch(logs []*raft.Log) []interface{} {
	var res []interface{}
	for _, l := range logs {
		res = append(res, b.Apply(l))
	}
	return res
}

// get from fsm
func (b *fsm) get(bucket, k []byte) (result []byte, found bool, e error) {
	return b.db.Get(bucket, k)
}

// pget from fsm
func (b *fsm) pget(bucket []byte, keys [][]byte) ([][]byte, error) {
	return b.db.PGet(bucket, keys)
}

// keys all in fsm
func (b *fsm) keys(bucket []byte) (keys [][]byte, values [][]byte, err error) {
	return b.db.Keys(bucket)
}

// keys all in fsm
func (b *fsm) keyStrings(bucket []byte) (keys []string, values [][]byte, err error) {
	return b.db.KeyStrings(bucket)
}

// keys all in fsm without return values
func (b *fsm) keysWithoutValues(bucket []byte) (keys [][]byte, err error) {
	return b.db.KeysWithoutValues(bucket)
}

// keys all in fsm without return values
func (b *fsm) keyStringsWithoutValues(bucket []byte) (keys []string, err error) {
	return b.db.KeyStringsWithoutValues(bucket)
}
