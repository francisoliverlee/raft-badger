package raft_badger

import (
	"bytes"
	"github.com/dgraph-io/badger/v4"
	kvstore "github.com/gmqio/kv-store"
	"io/ioutil"
	"os"
	"testing"
)

func testStableStore() *stableStore {
	fh, _ := ioutil.TempFile("", "stable_store")

	os.Remove(fh.Name())

	opt := badger.DefaultOptions(fh.Name())
	db, _ := kvstore.NewBadgerStore(opt)

	// Successfully creates and returns a store
	return newStableStore(db)
}

// test set get log config
func TestBadgerStore_Set_Get(t *testing.T) {
	store := testStableStore()

	defer func() {
		store.Close()
		for _, p := range store.db.Path() {
			os.Remove(p)
		}
	}()

	// Returns error on non-existent key
	if _, err := store.Get([]byte("bad")); err != nil {
		t.Fatalf("expected not found error, got: %q", err)
	}

	k, v := []byte("hello"), []byte("world")

	// Try to set a k/v pair
	if err := store.Set(k, v); err != nil {
		t.Fatalf("err: %s", err)
	}

	// Try to read it back
	val, err := store.Get(k)
	if err != nil {
		t.Fatalf("err: %s", err)
	}
	if !bytes.Equal(val, v) {
		t.Fatalf("bad: %v", val)
	}
}

// test uint64 to bytes
func TestBadgerStore_SetUint64_GetUint64(t *testing.T) {
	store := testStableStore()
	defer func() {
		store.Close()
		for _, p := range store.db.Path() {
			os.Remove(p)
		}
	}()

	// Returns error on non-existent key
	if _, err := store.GetUint64([]byte("bad")); err != nil {
		t.Fatalf("expected not found error, got: %q", err)
	}

	k, v := []byte("abc"), uint64(123)

	// Attempt to set the k/v pair
	if err := store.SetUint64(k, v); err != nil {
		t.Fatalf("err: %s", err)
	}

	// Read back the value
	val, err := store.GetUint64(k)
	if err != nil {
		t.Fatalf("err: %s", err)
	}
	if val != v {
		t.Fatalf("bad: %v", val)
	}
}
