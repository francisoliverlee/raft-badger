package raft_badger

import (
	"errors"
	"github.com/dgraph-io/badger/v4"
	"github.com/stretchr/testify/assert"
	"io/ioutil"
	"log"
	"os"
	"reflect"
	"testing"
	"time"

	"github.com/hashicorp/raft"
)

func testBadgerStore(t testing.TB) *logStore {
	fh, err := ioutil.TempFile("", "raft_badger_store")
	if err != nil {
		t.Fatalf("err: %s", err)
	}
	os.Remove(fh.Name())

	// Successfully creates and returns a store
	store, err := newLogStore(fh.Name(), false)
	if err != nil {
		t.Fatalf("err: %s", err)
	}

	return store
}

func testRaftLog(idx uint64, data string) *raft.Log {
	return &raft.Log{
		Data:  []byte(data),
		Index: idx,
	}
}

// test implement
func TestBadgerStore_Implements(t *testing.T) {
	var store interface{} = &logStore{}
	if _, ok := store.(raft.StableStore); !ok {
		t.Fatalf("ls does not implement raft.StableStore")
	}
	if _, ok := store.(raft.LogStore); !ok {
		t.Fatalf("ls does not implement raft.LogStore")
	}
}

// test read only
func TestBadgerOptionsReadOnly(t *testing.T) {
	fh, err := ioutil.TempFile("", "raft_badger_store")
	if err != nil {
		t.Fatalf("err: %s", err)
	}
	os.Remove(fh.Name())
	defer os.Remove(fh.Name())

	store, err := newLogStore(fh.Name(), false)

	if err != nil {
		t.Fatalf("err: %s", err)
	}
	// Create the log
	log := &raft.Log{
		Data:  []byte("log1"),
		Index: 1,
	}
	// Attempt to store the log
	if err := store.StoreLog(log); err != nil {
		t.Fatalf("err: %s", err)
	}

	store.Close()

	roStore, err := newLogStore(fh.Name(), true)
	if err != nil {
		t.Fatalf("err: %s", err)
	}
	defer roStore.Close()
	result := new(raft.Log)
	if err := roStore.GetLog(1, result); err != nil {
		t.Fatalf("err: %s", err)
	}

	// Ensure the log comes back the same
	if !reflect.DeepEqual(log, result) {
		t.Errorf("bad: %v", result)
	}
	// Attempt to store the log, should fail on a read-only store
	err = roStore.StoreLog(log)
	if err != badger.ErrReadOnlyTxn {
		t.Errorf("expecting error %v, but got %v", badger.ErrReadOnlyTxn, err)
	}
}

// test new raft badger store
func TestNewBadgerStore(t *testing.T) {
	fh, err := ioutil.TempFile("", "raft_badger_store")
	if err != nil {
		t.Fatalf("err: %s", err)
	}
	os.Remove(fh.Name())
	defer os.Remove(fh.Name())

	// Successfully creates and returns a store
	store, err := newLogStore(fh.Name(), false)
	if err != nil {
		t.Fatalf("err: %s", err)
	}

	if _, err := os.Stat(fh.Name()); err != nil {
		t.Fatalf("err: %s", err)
	}

	// Close the store so we can open again
	if err := store.Close(); err != nil {
		t.Fatalf("err: %s", err)
	}

}

// test update first index
func TestBadgerStore_FirstIndex(t *testing.T) {
	log.SetOutput(os.Stdout)

	store := testBadgerStore(t)
	defer func() {
		store.Close()
		for _, p := range store.db.Path() {
			os.Remove(p)
		}
	}()

	// Should get 0 index on empty log
	idx, err := store.FirstIndex()
	if err != nil {
		t.Fatalf("err: %s", err)
	}
	if idx != 0 {
		t.Fatalf("bad: %v", idx)
	}

	// Set a mock raft log
	logs := []*raft.Log{
		testRaftLog(1, "log1"),
		testRaftLog(2, "log2"),
		testRaftLog(3, "log3"),
	}
	if err := store.StoreLogs(logs); err != nil {
		t.Fatalf("bad: %s", err)
	}

	// Fetch the first Raft index
	idx, err = store.FirstIndex()
	if err != nil {
		t.Fatalf("err: %s", err)
	}
	if idx != 1 {
		t.Fatalf("bad: %d", idx)
	}
}

// test update latest index
func TestBadgerStore_LastIndex(t *testing.T) {
	store := testBadgerStore(t)
	defer func() {
		store.Close()
		for _, p := range store.db.Path() {
			os.Remove(p)
		}
	}()

	// Should get 0 index on empty log
	idx, err := store.LastIndex()
	if err != nil {
		t.Fatalf("err: %s", err)
	}
	if idx != 0 {
		t.Fatalf("bad: %v", idx)
	}

	// Set a mock raft log
	logs := []*raft.Log{
		testRaftLog(1, "log1"),
		testRaftLog(2, "log2"),
		testRaftLog(3, "log3"),
	}
	if err := store.StoreLogs(logs); err != nil {
		t.Fatalf("bad: %s", err)
	}

	// Fetch the last Raft index
	idx, err = store.LastIndex()
	if err != nil {
		t.Fatalf("err: %s", err)
	}
	if idx != 3 {
		t.Fatalf("bad: %d", idx)
	}
}

// test get non-exist log
// test store log, get log
func TestBadgerStore_GetLog(t *testing.T) {
	store := testBadgerStore(t)
	defer func() {
		store.Close()
		for _, p := range store.db.Path() {
			os.Remove(p)
		}
	}()

	log := new(raft.Log)

	// Should return an error on non-existent log
	if err := store.GetLog(1, log); !errors.Is(err, raft.ErrLogNotFound) {
		t.Fatalf("expected raft log not found error, got: %v", err)
	}

	// Set a mock raft log
	logs := []*raft.Log{
		testRaftLog(1, "log1"),
		testRaftLog(2, "log2"),
		testRaftLog(3, "log3"),
	}
	if err := store.StoreLogs(logs); err != nil {
		t.Fatalf("bad: %s", err)
	}

	// Should return the proper log
	if err := store.GetLog(2, log); err != nil {
		t.Fatalf("err: %s", err)
	}
	if !reflect.DeepEqual(log, logs[1]) {
		t.Fatalf("bad: %#v", log)
	}
}

// test log encode and decode
func TestBadgerStore_EncodeDecodeLog(t *testing.T) {
	log := &raft.Log{
		Index:      1,
		Term:       1000,
		Type:       raft.LogAddPeerDeprecated,
		Data:       []byte("log1"),
		Extensions: []byte("tiger extension"),
		AppendedAt: time.Now(),
	}

	v, err1 := encodeRaftLog(log)
	assert.True(t, err1 == nil)

	var log1 = &raft.Log{}
	assert.True(t, decodeRaftLog(v, log1) == nil)
	assert.True(t, log1 != nil)
	assert.True(t, log1.Index == log.Index)

}

// test set log
func TestBadgerStore_SetLog(t *testing.T) {
	store := testBadgerStore(t)
	defer func() {
		store.Close()
		for _, p := range store.db.Path() {
			os.Remove(p)
		}
	}()

	// Create the log
	log := &raft.Log{
		Data:  []byte("log1"),
		Index: 1,
	}

	// Attempt to store the log
	if err := store.StoreLog(log); err != nil {
		t.Fatalf("err: %s", err)
	}

	// Retrieve the log again
	result := new(raft.Log)
	if err := store.GetLog(1, result); err != nil {
		t.Fatalf("err: %s", err)
	}

	// Ensure the log comes back the same
	if !reflect.DeepEqual(log, result) {
		t.Fatalf("bad: %v", result)
	}
}

// test set logs
func TestBadgerStore_SetLogs(t *testing.T) {
	store := testBadgerStore(t)
	defer func() {
		store.Close()
		for _, p := range store.db.Path() {
			os.Remove(p)
		}
	}()

	// Create a set of logs
	logs := []*raft.Log{
		testRaftLog(1, "log1"),
		testRaftLog(2, "log2"),
	}

	// Attempt to store the logs
	if err := store.StoreLogs(logs); err != nil {
		t.Fatalf("err: %s", err)
	}

	// Ensure we stored them all
	result1, result2 := new(raft.Log), new(raft.Log)
	if err := store.GetLog(1, result1); err != nil {
		t.Fatalf("err: %s", err)
	}
	if !reflect.DeepEqual(logs[0], result1) {
		t.Fatalf("bad: %#v", result1)
	}
	if err := store.GetLog(2, result2); err != nil {
		t.Fatalf("err: %s", err)
	}
	if !reflect.DeepEqual(logs[1], result2) {
		t.Fatalf("bad: %#v", result2)
	}
}

// test add, delete logs, test first index and last index changed
func TestBadgerStore_DeleteRange(t *testing.T) {
	store := testBadgerStore(t)
	defer func() {
		store.Close()
		for _, p := range store.db.Path() {
			os.Remove(p)
		}
	}()

	// Create a set of logs
	log1 := testRaftLog(1, "log1")
	log2 := testRaftLog(2, "log2")
	log3 := testRaftLog(3, "log3")
	logs := []*raft.Log{log1, log2, log3}

	// Attempt to store the logs
	if err := store.StoreLogs(logs); err != nil {
		t.Fatalf("err: %s", err)
	}

	// Attempt to delete a range of logs
	if err := store.DeleteRange(1, 2); err != nil {
		t.Fatalf("err: %s", err)
	}

	// Ensure the logs were deleted
	if err := store.GetLog(1, new(raft.Log)); err != raft.ErrLogNotFound {
		t.Fatalf("should have deleted log1 %+v", err)
	}
	if err := store.GetLog(2, new(raft.Log)); err != raft.ErrLogNotFound {
		t.Fatalf("should have deleted log2")
	}

	// Ensure first index changed
	if fIdx, err := store.getFirstIndex(); fIdx != 3 {
		t.Fatalf("first index wrong after delete logs, should 3 but %d. %+v", fIdx, err)
	}

	// Ensure last index changed
	if lIdx, err := store.getLastIndex(); lIdx != 3 {
		t.Fatalf("last index wrong after add logs, should 3 but %d. %+v", lIdx, err)
	}
}
