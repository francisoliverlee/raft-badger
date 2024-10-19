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
	FsmCommandGet  = "Get"
	FsmCommandPGet = "PGet"

	FsmCommandSet  = "Set"
	FsmCommandPSet = "PSet"

	FsmCommandDel  = "Delete"
	FsmCommandPDel = "PDelete"

	FsmCommandKeys    = "Keys"    // keys or values in a bucket
	FsmCommandKeysAll = "KeysAll" // all keys
)

var (
	EmptyBucket       = []byte{}
	EmptyBucketLength = 0

	LogBucket       = []byte("_rbl_") // raft badger log
	LogBucketLength = len(LogBucket)

	ConfigBucket       = []byte("_rbl_cf_") // raft badger log config
	ConfigBucketLength = len(ConfigBucket)

	FsmBucket       = []byte("_fmsl_") // raft badger log config
	FsmBucketLength = len(FsmBucket)

	firstIndexKey = []byte("_first_k")
	lastIndexKey  = []byte("_last_k")

	cmd = map[string]interface{}{
		FsmCommandGet:  nil,
		FsmCommandPGet: nil,

		FsmCommandSet:  nil,
		FsmCommandPSet: nil,

		FsmCommandDel:  nil,
		FsmCommandPDel: nil,

		FsmCommandKeys:    nil,
		FsmCommandKeysAll: nil,
	}
)

type RaftStore struct {
	db kvstore.KvStore

	path string
}

type Stats struct {
}

type FsmCommand struct {
	Op    string `json:"op,omitempty"`
	Error error  `json:"-"` // only output, not input param,

	Bucket string            `json:"bucket,omitempty"` // check where to be used
	Kv     map[string]string `json:"kv_map,omitempty"`
}

func NewFsmCommand(op string) *FsmCommand {
	return &FsmCommand{
		Op: op,
		Kv: map[string]string{},
	}
}

type fsmSnapshot struct {
	store map[string]string
}

func NewBadgerStore(path string, readOnly bool) (*RaftStore, error) {
	opt := badger.DefaultOptions(path)
	opt.ReadOnly = readOnly

	db, err := kvstore.NewBadgerStore(opt)

	if err != nil {
		return nil, err
	}

	store := &RaftStore{
		db:   db,
		path: path,
	}

	return store, nil
}

func (c FsmCommand) ok() bool {
	_, ok := cmd[c.Op]
	return ok
}

func (b *RaftStore) ReadOnly() bool {
	return b.db.ReadOnly()
}

func (b *RaftStore) Path() string {
	return b.path
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

func (b *RaftStore) FirstIndex() (uint64, error) {
	return b.getFirstIndex()
}

func (b *RaftStore) LastIndex() (uint64, error) {
	return b.getLastIndex()
}

func (b *RaftStore) GetLog(idx uint64, log *raft.Log) error {
	val, found, err := b.db.Get(LogBucket, uint64ToBytes(idx))
	if !found {
		return raft.ErrLogNotFound
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
		idxB := uint64ToBytes(rlog.Index)

		newKey := kvstore.AppendBytes(LogBucketLength+8, LogBucket, idxB)
		val, err := encodeRaftLog(rlog)

		if err != nil {
			return err
		}

		keys = append(keys, newKey)
		values = append(values, val)

		if rlog.Index == 1 { // set first index
			keys = append(keys, firstIndexKey)
			values = append(values, uint64ToBytes(rlog.Index))
		}
		// set last index
		keys = append(keys, lastIndexKey)
		values = append(values, uint64ToBytes(rlog.Index))
	}

	return b.db.PSet(EmptyBucket, keys, values)
}

func (b *RaftStore) DeleteRange(minIdx, maxIdx uint64) error {
	diff := maxIdx - minIdx
	if diff <= 0 {
		return errors.New(fmt.Sprintf("op fail. max index[%d] should bigger than min index[%d]", maxIdx, minIdx))
	}
	// TODO optimise when diff very big, such as 1000K
	return b.db.Exec(func(txn *badger.Txn) error {
		for i := minIdx; i <= maxIdx; i++ {
			newKey := kvstore.AppendBytes(LogBucketLength+8, LogBucket, uint64ToBytes(i))
			if err := txn.Delete(newKey); err != nil {
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
	v, _, err := b.db.Get(EmptyBucket, k)
	if err != nil {
		return nil, err
	}
	return v, err
}

func (b *RaftStore) GetAppliedValue(k string) ([]byte, error) {
	keyB := []byte(k)
	newKey := kvstore.AppendBytes(FsmBucketLength+len(keyB), FsmBucket, keyB)
	return b.GetFromBadger(newKey)
}

func (b *RaftStore) Apply(l *raft.Log) interface{} {
	var c FsmCommand
	if err := json.Unmarshal(l.Data, &c); err != nil {
		c.Error = errors2.Wrap(err, fmt.Sprintf("failed to apply. unmarshal log error, index: %d", l.Index))
		return c
	}
	if c.Kv == nil || len(c.Kv) == 0 {
		c.Error = errors.New(fmt.Sprintf("failed to apply. empty kv, index=%d", l.Index))
		return c
	}
	if !c.ok() {
		c.Error = errors.New(fmt.Sprintf("failed to apply, cmd op error,op=%s, index=%d", c.Op, l.Index))
		return c
	}

	// prepare params
	multiKeys := false
	var newKeys [][]byte
	var newValues [][]byte
	for k, v := range c.Kv {
		keyB := []byte(k)
		newKey := kvstore.AppendBytes(FsmBucketLength+len(keyB), FsmBucket, keyB)

		switch c.Op {
		case FsmCommandSet:
			if err := b.db.Set(FsmBucket, newKey, []byte(v)); err != nil {
				c.Error = errors2.Wrap(err, fmt.Sprintf("failed to apply %s, newKey: %s", c.Op, string(newKey)))
			}
		case FsmCommandDel:
			if err := b.db.Delete(FsmBucket, newKey); err != nil {
				c.Error = errors2.Wrap(err, fmt.Sprintf("failed to apply %s, newKey: %s", c.Op, string(newKey)))
			}
		case FsmCommandGet:
			if val, _, err := b.db.Get(FsmBucket, newKey); err != nil {
				c.Error = errors2.Wrap(err, fmt.Sprintf("failed to apply %s, newKey: %s", c.Op, string(newKey)))
			} else {
				c.Kv[k] = string(val)
			}
		case FsmCommandPSet:
			if newKeys == nil {
				newKeys = make([][]byte, 0)
			}
			if newValues == nil {
				newValues = make([][]byte, 0)
			}
			newKeys = append(newKeys, newKey)
			newValues = append(newValues, []byte(v))
			multiKeys = true
			continue
		case FsmCommandPDel:
		case FsmCommandPGet:
			if newKeys == nil {
				newKeys = make([][]byte, 0)
			}
			newKeys = append(newKeys, newKey)
			multiKeys = true
			continue
		}
		break
	}

	// return , if error happened when single key or multi key
	if c.Error != nil {
		return c
	}

	// multi keys
	if multiKeys {
		switch c.Op {
		case FsmCommandPSet:
			if err := b.db.PSet(FsmBucket, newKeys, newValues); err != nil {
				c.Error = errors2.Wrap(err, fmt.Sprintf("failed to apply "+c.Op))
			}
		case FsmCommandPGet:
			if vals, err := b.db.PGet(FsmBucket, newKeys); err != nil {
				c.Error = errors2.Wrap(err, fmt.Sprintf("failed to apply "+c.Op))
			} else {
				if len(newKeys) != len(vals) {
					c.Error = errors2.Wrap(err, fmt.Sprintf("failed to apply "+c.Op))
				} else {
					for i, key := range newKeys {
						c.Kv[string(key)] = string(vals[i])
					}
				}
			}
		case FsmCommandPDel:
			if err := b.db.DeleteKeys(FsmBucket, newKeys); err != nil {
				c.Error = errors2.Wrap(err, fmt.Sprintf("failed to apply "+c.Op))
			}
		}
		return c
	}

	// unknown keys in bucket
	switch c.Op {
	case FsmCommandKeys:
		bb := []byte(c.Bucket)
		nbb := kvstore.AppendBytes(FsmBucketLength+len(bb), FsmBucket, bb)
		if keys, vals, err := b.db.Keys(nbb); err != nil {
			c.Error = errors2.Wrap(err, fmt.Sprintf("failed to apply %s, new bucket: %s", c.Op, string(nbb)))
			return c
		} else {
			newKeys = keys
			newValues = vals
		}

		if newKeys == nil || len(newKeys) == 0 || newValues == nil || len(newValues) == 0 {
			log.Printf("empty in bucket " + c.Bucket)
			return c
		}

		if len(newKeys) != len(newValues) {
			log.Fatalf("[BUG] length of keys[%d] and values[%d] not the same in bucket %s", len(newKeys), len(newValues), c.Bucket)
			return c
		}
		for i, key := range newKeys {
			c.Kv[string(key)] = string(newValues[i])
		}
	}

	return c
}

func (b *RaftStore) ApplyBatch(logs []*raft.Log) []interface{} {
	var res []interface{}
	for _, l := range logs {
		res = append(res, b.Apply(l))
	}
	return res
}

// Snapshot The Snapshot implementation should return quickly, because Apply can not
// be called while Snapshot is running
func (b *RaftStore) Snapshot() (raft.FSMSnapshot, error) {
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

func (b *RaftStore) Restore(rc io.ReadCloser) error {
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

func (b *RaftStore) toLogString(l *raft.Log) string {
	return fmt.Sprintf("idx=%d,type=%s, appended_at=%s, data=%s,term=%d, ext=%s", l.Index, l.Type.String(), l.AppendedAt.String(), string(l.Data), l.Term, string(l.Extensions))
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
