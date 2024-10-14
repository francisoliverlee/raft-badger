package raft_badger

import (
	"errors"
	"fmt"
	"github.com/dgraph-io/badger/v4"
	"github.com/hashicorp/raft"
)

// for raft log
type logStore BadgerStore

// for first and last raft log entry index
func (b *logStore) setFirstIndex(tx *badger.Txn, first uint64) error {
	l("set key=%s, val=%d", string(firstIndexKey), first)
	return tx.Set(firstIndexKey, uint64ToBytes(first))
}

func (b *logStore) getFirstIndex() (uint64, error) {
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

func (b *logStore) setLastIndex(tx *badger.Txn, last uint64) error {
	l("set key=%s, val=%d", string(lastIndexKey), last)
	return tx.Set(lastIndexKey, uint64ToBytes(last))
}

func (b *logStore) getLastIndex() (uint64, error) {
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

func (b *logStore) Close() error {
	return b.db.Close()
}

func (b *logStore) FirstIndex() (uint64, error) {
	return b.getFirstIndex()
}

func (b *logStore) LastIndex() (uint64, error) {
	return b.getLastIndex()
}

func (b *logStore) GetLog(idx uint64, log *raft.Log) error {
	return b.db.View(func(tx *badger.Txn) error {
		key := encodeRaftLogKey(idx)

		if item, err := tx.Get(key); err != nil {
			if errors.Is(err, badger.ErrKeyNotFound) {
				return raft.ErrLogNotFound
			}
			return err
		} else {
			return item.Value(func(val []byte) error {
				if val == nil || len(val) == 0 {
					return errors.New(fmt.Sprintf("value nil for key=%s", string(key)))
				}
				tmpLog, dErr := decodeRaftLog(val)
				if dErr != nil {
					return dErr
				}
				log.Index = tmpLog.Index
				log.Term = tmpLog.Term
				log.Type = tmpLog.Type
				log.Data = tmpLog.Data
				log.Extensions = tmpLog.Extensions

				return err
			})
		}
	})
}

func (b *logStore) StoreLog(log *raft.Log) error {
	return b.StoreLogs([]*raft.Log{log})
}

func (b *logStore) StoreLogs(logs []*raft.Log) error {
	return b.db.Update(func(tx *badger.Txn) error {
		for _, rlog := range logs {
			key := encodeRaftLogKey(rlog.Index)
			val, err := encodeRaftLog(rlog)
			if err != nil {
				l("encodeRaftLog error. %s, %v", string(key), rlog)
				return err
			}
			l("set key=%s, val=%v", string(key), rlog)
			if err := tx.Set(key, val); err != nil {
				return err
			}
			if rlog.Index == 1 {
				if err := b.setFirstIndex(tx, rlog.Index); err != nil {
					return err
				}
			}
			if err := b.setLastIndex(tx, rlog.Index); err != nil {
				return err
			}
		}
		return nil
	})
}

func (b *logStore) DeleteRange(minIdx, maxIdx uint64) error {
	diff := maxIdx - minIdx
	if diff <= 0 {
		return errors.New(fmt.Sprintf("op fail. max index[%d] should bigger than min index[%d]", maxIdx, minIdx))
	}
	// TODO optimise when diff very big, such as 1000K
	return b.db.Update(func(txn *badger.Txn) error {
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

// GetFromBadger for raft-badger self
func (b *logStore) GetFromBadger(k []byte) (val []byte, err error) {
	err = b.db.View(func(txn *badger.Txn) error {
		if item, e := txn.Get(k); e != nil {
			return e
		} else {
			return item.Value(func(v []byte) error {
				if v == nil || len(v) == 0 {
					return errors.New(fmt.Sprintf("value nil for key=%s", string(k)))
				}
				val = v
				return nil
			})
		}
	})
	return val, err
}
