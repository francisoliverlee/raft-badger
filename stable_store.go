package raft_badger

import (
	"errors"
	"fmt"
	"github.com/dgraph-io/badger/v4"
)

type stableStore BadgerStore

func (b *stableStore) Set(k, v []byte) error {
	l("set key=%s, val=%v", string(k), string(v))
	return b.db.Update(func(txn *badger.Txn) error {
		key := encodeKey(dbConfPrefixKey, k)
		return txn.Set(key, v)
	})
}

func (b *stableStore) Get(k []byte) (val []byte, err error) {
	err = b.db.View(func(txn *badger.Txn) error {
		key := encodeKey(dbConfPrefixKey, k)

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

func (b *stableStore) SetUint64(key []byte, val uint64) error {
	return b.Set(key, uint64ToBytes(val))
}

func (b *stableStore) GetUint64(key []byte) (uint64, error) {
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
