// raft log store

package raft_badger

import (
	"errors"
	"fmt"
	"github.com/dgraph-io/badger/v4"
	kvstore "github.com/gmqio/kv-store"
	"github.com/hashicorp/raft"
)

const (
	FsmCommandSet = "Set"
	FsmCommandDel = "Delete"
)

var (
	EmptyBucket       = []byte{}
	EmptyBucketLength = len(EmptyBucket)

	LogBucket       = []byte("_rlb_") // raft log bucket
	LogBucketLength = len(LogBucket)

	StableBucket       = []byte("_rsl_") // raft stable log
	StableBucketLength = len(StableBucket)

	FsmBucket       = []byte("_rfb_") // raft fsm bucket
	FsmBucketLength = len(FsmBucket)

	firstIndexKey = []byte("_first_k")
	lastIndexKey  = []byte("_last_k")

	FsmCmd = map[string]interface{}{
		FsmCommandSet: nil,
		FsmCommandDel: nil,
	}
)

type logStore struct {
	db kvstore.KvStore
}

type Stats struct {
}

func newLogStore(path string, readOnly bool) (*logStore, error) {
	opt := badger.DefaultOptions(path)
	opt.ReadOnly = readOnly

	db, err := kvstore.NewBadgerStore(opt)

	if err != nil {
		return nil, err
	}

	store := &logStore{
		db: db,
	}

	return store, nil
}

func (b *logStore) ReadOnly() bool {
	return b.db.ReadOnly()
}

func (b *logStore) Path() []string {
	return b.db.Path()
}

func (b *logStore) setFirstIndex(tx *badger.Txn, first uint64) error {
	return tx.Set(firstIndexKey, uint64ToBytes(first))
}

func (b *logStore) getFirstIndex() (uint64, error) {
	val, err := b.R(firstIndexKey)
	if err == nil {
		return bytesToUint64(val), nil
	} else {
		if errors.Is(err, kvstore.KeyNotFoundError) {
			return 0, nil
		}
		return 0, err
	}
}

func (b *logStore) setLastIndex(tx *badger.Txn, last uint64) error {
	return tx.Set(lastIndexKey, uint64ToBytes(last))
}

func (b *logStore) getLastIndex() (uint64, error) {
	val, err := b.R(lastIndexKey)
	if err == nil {
		return bytesToUint64(val), nil
	} else {
		if errors.Is(err, kvstore.KeyNotFoundError) {
			return 0, nil
		}
		return 0, err
	}
}

func (b *logStore) FirstIndex() (uint64, error) {
	return b.getFirstIndex()
}

func (b *logStore) LastIndex() (uint64, error) {
	return b.getLastIndex()
}

func (b *logStore) GetLog(idx uint64, log *raft.Log) error {
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

func (b *logStore) StoreLog(log *raft.Log) error {
	return b.StoreLogs([]*raft.Log{log})
}

func (b *logStore) StoreLogs(logs []*raft.Log) error {
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

func (b *logStore) DeleteRange(minIdx, maxIdx uint64) error {
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

func (b *logStore) Close() error {
	return b.db.Close()
}

func (b *logStore) R(k []byte) (val []byte, err error) { // R for real
	v, _, err := b.db.Get(EmptyBucket, k)
	if err != nil {
		return nil, err
	}
	return v, err
}

func (b *logStore) toLogString(l *raft.Log) string {
	return fmt.Sprintf("idx=%d,type=%s, appended_at=%s, data=%s,term=%d, ext=%s", l.Index, l.Type.String(), l.AppendedAt.String(), string(l.Data), l.Term, string(l.Extensions))
}

func (b *logStore) Sync() error {
	return b.db.Sync()
}

func (b *logStore) Stats() Stats {
	return Stats{}
}
