package raft_badger

import (
	"errors"
	"fmt"
	"github.com/dgraph-io/badger/v4"
	"github.com/hashicorp/raft"
	"log"
	"os"
)

const (
	debugFlag = "raftBadgerDebug"
)

var (
	dbLogPrefixKey  = []byte("_rbl_")    // raft badger log
	dbConfPrefixKey = []byte("_rbl_cf_") // raft badger log config

	firstIndexKey = []byte("_first_k")
	lastIndexKey  = []byte("_last_k")

	debugVal = os.Getenv(debugFlag)
)

type BadgerStore struct {
	db     *badger.DB
	option *Options
}

type Stats struct {
}

type Options struct {
	BadgerOptions               *badger.Options
	BatchSizeOfDeleteLogEntries int
}

func (o *Options) readOnly() bool {
	return o != nil && o.BadgerOptions != nil && o.BadgerOptions.ReadOnly
}

func NewBadgerStore(path string) (*BadgerStore, error) {
	var defOp = badger.DefaultOptions(path)
	return New(&Options{
		BadgerOptions: &defOp,
	})
}

func New(ops *Options) (*BadgerStore, error) {
	db, err := badger.Open(*ops.BadgerOptions)
	if err != nil {
		return nil, err
	}

	store := &BadgerStore{
		db:     db,
		option: ops,
	}

	return store, nil
}

// for first and last raft log entry index

func (b *BadgerStore) setFirstIndex(tx *badger.Txn, first uint64) error {
	l("set key=%s, val=%d", string(firstIndexKey), first)
	return tx.Set(firstIndexKey, uint64ToBytes(first))
}

func (b *BadgerStore) getFirstIndex() (uint64, error) {
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

func (b *BadgerStore) setLastIndex(tx *badger.Txn, last uint64) error {
	l("set key=%s, val=%d", string(lastIndexKey), last)
	return tx.Set(lastIndexKey, uint64ToBytes(last))
}

func (b *BadgerStore) getLastIndex() (uint64, error) {
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

// for raft log

func (b *BadgerStore) Close() error {
	return b.db.Close()
}

func (b *BadgerStore) FirstIndex() (uint64, error) {
	return b.getFirstIndex()
}

func (b *BadgerStore) LastIndex() (uint64, error) {
	return b.getLastIndex()
}

func (b *BadgerStore) GetLog(idx uint64, log *raft.Log) error {
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

func (b *BadgerStore) StoreLog(log *raft.Log) error {
	return b.StoreLogs([]*raft.Log{log})
}

func (b *BadgerStore) StoreLogs(logs []*raft.Log) error {
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

func (b *BadgerStore) DeleteRange(minIdx, maxIdx uint64) error {
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

// for config kv

func (b *BadgerStore) Set(k, v []byte) error {
	l("set key=%s, val=%v", string(k), string(v))
	return b.db.Update(func(txn *badger.Txn) error {
		key := encodeConfigKey(dbConfPrefixKey, k)
		return txn.Set(key, v)
	})
}

func (b *BadgerStore) Get(k []byte) (val []byte, err error) {
	err = b.db.View(func(txn *badger.Txn) error {
		key := encodeConfigKey(dbConfPrefixKey, k)

		if item, e := txn.Get(key); e != nil {
			return e
		} else {
			return item.Value(func(v []byte) error {
				if v == nil || len(v) == 0 {
					return errors.New(fmt.Sprintf("value nil for key=%s", string(key)))
				}
				val = v
				return nil
			})
		}
	})

	if errors.Is(err, badger.ErrKeyNotFound) {
		val = []byte{}
		err = nil
	}
	return val, err
}

func (b *BadgerStore) SetUint64(key []byte, val uint64) error {
	return b.Set(key, uint64ToBytes(val))
}

func (b *BadgerStore) GetUint64(key []byte) (uint64, error) {
	val, err := b.Get(key)
	if err != nil {
		return 0, err
	}
	if len(val) == 0 {
		l("key[%s] not found or value empty", string(key))
		return 0, nil
	}
	return bytesToUint64(val), nil
}

// for raft-badger self

func (b *BadgerStore) GetFromBadger(k []byte) (val []byte, err error) {
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

func (b *BadgerStore) Sync() error {
	return b.db.Sync()
}

func (b *BadgerStore) Stats() Stats {
	return Stats{}
}

func l(format string, v ...any) {
	if Debug(debugVal) {
		log.Printf(format, v...)
	}
}
