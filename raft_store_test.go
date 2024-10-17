package raft_badger

import (
	"bytes"
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

func testBadgerStore(t testing.TB) *RaftStore {
	fh, err := ioutil.TempFile("", "raft_badger_store")
	if err != nil {
		t.Fatalf("err: %s", err)
	}
	os.Remove(fh.Name())

	// Successfully creates and returns a store
	store, err := NewBadgerStore(fh.Name())
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

func testGenOption(path string) *Options {
	op := badger.DefaultOptions(path)
	return &Options{
		BadgerOptions:               &op,
		BatchSizeOfDeleteLogEntries: 100,
	}
}

func TestBadgerStore_Implements(t *testing.T) {
	var store interface{} = &RaftStore{}
	if _, ok := store.(raft.StableStore); !ok {
		t.Fatalf("RaftStore does not implement raft.StableStore")
	}
	if _, ok := store.(raft.LogStore); !ok {
		t.Fatalf("RaftStore does not implement raft.LogStore")
	}
}

func TestBadgerOptionsReadOnly(t *testing.T) {
	fh, err := ioutil.TempFile("", "raft_badger_store")
	if err != nil {
		t.Fatalf("err: %s", err)
	}
	os.Remove(fh.Name())
	defer os.Remove(fh.Name())

	op := testGenOption(fh.Name())

	store, err := New(op)

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
	options := testGenOption(fh.Name())
	options.BadgerOptions.ReadOnly = true
	roStore, err := New(options)
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

func TestNewBadgerStore(t *testing.T) {
	fh, err := ioutil.TempFile("", "raft_badger_store")
	if err != nil {
		t.Fatalf("err: %s", err)
	}
	os.Remove(fh.Name())
	defer os.Remove(fh.Name())

	// Successfully creates and returns a store
	store, err := NewBadgerStore(fh.Name())
	if err != nil {
		t.Fatalf("err: %s", err)
	}

	// Ensure the file was created
	if store.option.BadgerOptions.Dir != fh.Name() {
		t.Fatalf("unexpected file path %q", store.option.BadgerOptions.Dir)
	}
	if _, err := os.Stat(fh.Name()); err != nil {
		t.Fatalf("err: %s", err)
	}

	// Close the store so we can open again
	if err := store.Close(); err != nil {
		t.Fatalf("err: %s", err)
	}

	// Ensure our tables were created
	var op = testGenOption(fh.Name())
	db, err := badger.Open(*op.BadgerOptions)
	if err != nil {
		t.Fatalf("err: %s", err)
	}
	db.Sync()
}

func TestBadgerStore_FirstIndex(t *testing.T) {
	log.SetOutput(os.Stdout)

	store := testBadgerStore(t)
	defer store.Close()
	defer os.Remove(store.option.BadgerOptions.Dir)

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

func TestBadgerStore_LastIndex(t *testing.T) {
	store := testBadgerStore(t)
	defer store.Close()
	defer os.Remove(store.option.BadgerOptions.Dir)

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

func TestBadgerStore_GetLog(t *testing.T) {
	store := testBadgerStore(t)
	defer store.Close()
	defer os.Remove(store.option.BadgerOptions.Dir)

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

	var log1 *raft.Log
	assert.True(t, decodeRaftLog(v, log1) == nil)
	assert.True(t, log1 != nil)
	assert.True(t, log1.Index == log.Index)

}

func TestBadgerStore_SetLog(t *testing.T) {
	store := testBadgerStore(t)
	defer store.Close()
	defer os.Remove(store.option.BadgerOptions.Dir)

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

func TestBadgerStore_SetLogs(t *testing.T) {
	store := testBadgerStore(t)
	defer store.Close()
	defer os.Remove(store.option.BadgerOptions.Dir)

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

func TestBadgerStore_DeleteRange(t *testing.T) {
	store := testBadgerStore(t)
	defer store.Close()
	defer os.Remove(store.option.BadgerOptions.Dir)

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
		t.Fatalf("should have deleted log1")
	}
	if err := store.GetLog(2, new(raft.Log)); err != raft.ErrLogNotFound {
		t.Fatalf("should have deleted log2")
	}
}

func TestBadgerStore_Set_Get(t *testing.T) {
	store := testBadgerStore(t)
	defer store.Close()
	defer os.Remove(store.option.BadgerOptions.Dir)

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

func TestBadgerStore_SetUint64_GetUint64(t *testing.T) {
	store := testBadgerStore(t)
	defer store.Close()
	defer os.Remove(store.option.BadgerOptions.Dir)

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
