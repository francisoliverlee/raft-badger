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
	FsmCommandGet    = "get"
	FsmCommandSet    = "set"
	FsmCommandDelete = "del"
)

var (
	Emptybucket  = []byte{}
	LogBucket    = []byte("_rbl_")    // raft badger log
	ConfigBucket = []byte("_rbl_cf_") // raft badger log config

	FsmBucket = []byte("_fmsl_") // raft badger log config

	firstIndexKey = []byte("_first_k")
	lastIndexKey  = []byte("_last_k")
)

type RaftStore struct {
	db kvstore.KvStore
}

type Stats struct {
}

type FsmCommand struct {
	Op    string `json:"op,omitempty"`
	Key   string `json:"key,omitempty"`
	Value string `json:"value,omitempty"`

	Error error `json:"-"` // response
}

type fsmSnapshot struct {
	store map[string]string
}

func (o *RaftStore) readOnly() bool {
	return o.db.ReadOnly()
}

func NewBadgerStore(path string) (*RaftStore, error) {
	db, err := kvstore.NewBadgerStore(badger.DefaultOptions(path))

	if err != nil {
		return nil, err
	}

	store := &RaftStore{
		db: db,
	}

	return store, nil
}

func (b *RaftStore) Set(k, v []byte) error {
	return b.db.Set(ConfigBucket, k, v)
}

func (b *RaftStore) Get(k []byte) (val []byte, e error) {
	val, _, err := b.db.Get(ConfigBucket, k)

	if errors.Is(err, badger.ErrKeyNotFound) {
		val = []byte{}
		err = nil
	}
	return val, err
}

func (b *RaftStore) SetUint64(key []byte, val uint64) error {
	return b.Set(key, uint64ToBytes(val))
}

func (b *RaftStore) GetUint64(key []byte) (uint64, error) {
	val, err := b.Get(key)
	if err != nil {
		return 0, err
	}
	if len(val) == 0 {

		return 0, nil
	}
	return bytesToUint64(val), nil
}

func (b *RaftStore) setFirstIndex(tx *badger.Txn, first uint64) error {
	return tx.Set(firstIndexKey, uint64ToBytes(first))
}

func (b *RaftStore) getFirstIndex() (uint64, error) {
	val, err := b.GetFromBadger(firstIndexKey)
	if err == nil {
		return bytesToUint64(val), nil
	} else {
		if errors.Is(err, badger.ErrKeyNotFound) {
			return 0, nil
		}
		return 0, err
	}
}

func (b *RaftStore) setLastIndex(tx *badger.Txn, last uint64) error {
	return tx.Set(lastIndexKey, uint64ToBytes(last))
}

func (b *RaftStore) getLastIndex() (uint64, error) {
	val, err := b.GetFromBadger(lastIndexKey)
	if err == nil {
		return bytesToUint64(val), nil
	} else {
		if errors.Is(err, badger.ErrKeyNotFound) {
			return 0, nil
		}
		return 0, err
	}
}

func (b *RaftStore) FirstIndex() (uint64, error) {
	return b.getFirstIndex()
}

func (b *RaftStore) LastIndex() (uint64, error) {
	return b.getLastIndex()
}

func (b *RaftStore) GetLog(idx uint64, log *raft.Log) error {
	val, found, err := b.db.Get(LogBucket, uint64ToBytes(idx))
	if !found {
		return errors.New(fmt.Sprintf("raft log not found for idx=%d", idx))
	}
	if err != nil {
		return errors.New(fmt.Sprintf("get raft log error for idx=%d", idx))
	}
	if val == nil || len(val) == 0 {
		return errors.New(fmt.Sprintf("raft log is nil for idx=%d", idx))
	}
	return decodeRaftLog(val, log)
}

func (b *RaftStore) StoreLog(log *raft.Log) error {
	return b.StoreLogs([]*raft.Log{log})
}

func (b *RaftStore) StoreLogs(logs []*raft.Log) error {
	var keys [][]byte
	var values [][]byte

	for _, rlog := range logs {
		key := uint64ToBytes(rlog.Index)
		val, err := encodeRaftLog(rlog)
		if err != nil {
			return err
		}

		keys = append(keys, key)
		values = append(values, val)

		if rlog.Index == 1 { // set first index
			keys = append(keys, firstIndexKey)
			values = append(values, uint64ToBytes(rlog.Index))
		}
		// set last index
		keys = append(keys, lastIndexKey)
		values = append(values, uint64ToBytes(rlog.Index))
	}

	return b.db.PSet(Emptybucket, keys, values)
}

func (b *RaftStore) DeleteRange(minIdx, maxIdx uint64) error {
	diff := maxIdx - minIdx
	if diff <= 0 {
		return errors.New(fmt.Sprintf("op fail. max index[%d] should bigger than min index[%d]", maxIdx, minIdx))
	}
	// TODO optimise when diff very big, such as 1000K
	return b.db.Exec(func(txn *badger.Txn) error {
		for i := minIdx; i <= maxIdx; i++ {
			key := encodeRaftLogKey(i)
			if err := txn.Delete(key); err != nil {
				return err
			}
		}

		if err := b.setFirstIndex(txn, maxIdx+1); err != nil {
			return err
		}
		return nil
	})
}

func (b *RaftStore) GetFromBadger(k []byte) (val []byte, err error) {
	v, _, err := b.db.Get(Emptybucket, k)
	if err != nil {
		return nil, err
	}
	return v, err
}

func (f *RaftStore) GetAppliedValue(k string) ([]byte, error) {
	key := encodeKey(FsmBucket, []byte(k))
	return f.GetFromBadger(key)
}

func (f *RaftStore) Apply(l *raft.Log) interface{} {
	var c FsmCommand
	if err := json.Unmarshal(l.Data, &c); err != nil {
		c.Error = errors2.Wrap(err, fmt.Sprintf("failed to do apply unmarshal, key: %s", c.Key))
		return c
	}

	key := encodeKey(FsmBucket, []byte(c.Key))

	switch c.Op {
	case FsmCommandSet:
		if err := f.db.Set(FsmBucket, key, []byte(c.Value)); err != nil {
			c.Error = errors2.Wrap(err, fmt.Sprintf("failed to apply set key: %s", string(key)))
		}
	case FsmCommandDelete:
		if err := f.db.Delete(FsmBucket, key); err != nil {
			c.Error = errors2.Wrap(err, fmt.Sprintf("failed to apply delete key: %s", string(key)))
		}
	case FsmCommandGet:
		if val, _, err := f.db.Get(FsmBucket, key); err != nil {
			c.Error = errors2.Wrap(err, fmt.Sprintf("failed to apply get %s %v", string(key), err))
		} else {
			c.Value = string(val)
		}
	default:
		c.Error = errors.New(fmt.Sprintf("unknown command: %s. key: %s ", c.Op, c.Key))
	}

	return c
}

// Snapshot The Snapshot implementation should return quickly, because Apply can not
// be called while Snapshot is running
func (f *RaftStore) Snapshot() (raft.FSMSnapshot, error) {
	kvMap := map[string]string{}

	if keys, vals, err := f.db.KeyStrings(FsmBucket); err != nil {
		return &fsmSnapshot{store: kvMap}, err
	} else {
		for i, k := range keys {
			kvMap[k] = string(vals[i])
		}
	}

	return &fsmSnapshot{store: kvMap}, nil
}

func (f *RaftStore) Restore(rc io.ReadCloser) error {
	newKv := make(map[string]string)
	if err := json.NewDecoder(rc).Decode(&newKv); err != nil {
		return err
	}

	s, sEr := f.Snapshot()
	if sEr != nil {
		log.Printf("Restore warn, get snapshot error. %s.  would replace all kv", sEr)
	}
	var fsmS = s.(*fsmSnapshot)
	PrintMapDiff(fsmS.store, newKv)

	// NOTICE : big newKv would spend lots of time
	return f.db.Exec(func(txn *badger.Txn) error {
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

func (f *fsmSnapshot) Release() {
	f.store = map[string]string{}
}

func (b *RaftStore) Close() error {
	return b.db.Close()
}

func (b *RaftStore) Sync() error {
	return b.db.Sync()
}

func (b *RaftStore) Stats() Stats {
	return Stats{}
}
