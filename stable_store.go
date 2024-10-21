package raft_badger

import (
	"errors"
	kvstore "github.com/gmqio/kv-store"
)

type stableStore struct {
	db kvstore.KvStore
}

func newStableStore(db kvstore.KvStore) *stableStore {
	return &stableStore{
		db: db,
	}
}

func (b *stableStore) Set(k, v []byte) error {
	return b.db.Set(StableBucket, k, v)
}

// Get returns the value for key, or an empty byte slice if key was not found.
func (b *stableStore) Get(k []byte) (val []byte, e error) {
	val, _, err := b.db.Get(StableBucket, k)
	if errors.Is(err, kvstore.KeyNotFoundError) {
		val = []byte{}
		err = nil
	}
	return val, err
}

func (b *stableStore) SetUint64(key []byte, val uint64) error {
	return b.Set(key, uint64ToBytes(val))
}

// GetUint64 returns the uint64 value for key, or 0 if key was not found.
func (b *stableStore) GetUint64(key []byte) (uint64, error) {
	val, err := b.Get(key)
	if err != nil {
		return 0, err
	}
	if len(val) == 0 {
		return 0, nil
	}
	return bytesToUint64(val), nil
}

func (b *stableStore) Close() error {
	return b.db.Close()
}
