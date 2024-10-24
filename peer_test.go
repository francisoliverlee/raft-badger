package raft_badger

import (
	"encoding/json"
	"errors"
	"github.com/hashicorp/raft"
	"github.com/stretchr/testify/assert"
	"io/ioutil"
	"log"
	"os"
	"path/filepath"
	"testing"
	"time"
)

type config struct {
	id       string
	raftAddr string
	dataPath string
}

// Test_StoreOpen tests that the store can be opened single node
func Test_StoreOpen1(t *testing.T) {
	tmpDir, _ := ioutil.TempDir("", "store_test")
	s := NewPeer(tmpDir, "127.0.0.1:0")
	s.RaftDir = tmpDir
	defer func() {
		_ = os.RemoveAll(tmpDir)
	}()

	if s == nil {
		t.Fatalf("failed to create store")
	}

	if err := s.Open("node0"); err != nil {
		t.Fatalf("failed to open store: %s", err)
	}
}

// Test_StoreOpen tests that the store can be opened multi node
func Test_StoreOpen2(t *testing.T) {
	tmpDir, _ := ioutil.TempDir("", "store_test")
	s := NewPeer(tmpDir, "127.0.0.1:0")
	s.RaftDir = tmpDir
	defer func() {
		_ = os.RemoveAll(tmpDir)
	}()

	if s == nil {
		t.Fatalf("failed to create store")
	}

	if err := s.Open("node0"); err != nil {
		t.Fatalf("failed to open store: %s", err)
	}
}

// Test_StoreOpenSingleNode tests that a command can be applied to the log
func Test_StoreOpenSingleNode(t *testing.T) {
	tmpDir, _ := ioutil.TempDir("", "store_test")
	s := NewPeer(tmpDir, "127.0.0.1:30001")
	s.RaftDir = tmpDir
	defer func() {
		_ = os.RemoveAll(tmpDir)
	}()
	if s == nil {
		t.Fatalf("failed to create store")
	}

	if err := s.Open("node0"); err != nil {
		t.Fatalf("failed to open store: %s", err)
	}

	// Simple way to ensure there is a leader.
	time.Sleep(3 * time.Second)

	if err := s.Set("tiger-bucket", "foo", "bar"); err != nil {
		t.Fatalf("failed to set key: %s", err.Error())
	}

	// Wait for committed log entry to be applied.
	time.Sleep(500 * time.Millisecond)
	value, _, err := s.Get("tiger-bucket", "foo")
	if err != nil {
		t.Fatalf("failed to get key: %s", err.Error())
	}
	if value != "bar" {
		t.Fatalf("key has wrong value: %s", value)
	}

	if err := s.Delete("tiger-bucket", "foo"); err != nil {
		t.Fatalf("failed to delete key: %s", err.Error())
	}

	// Wait for committed log entry to be applied.
	time.Sleep(500 * time.Millisecond)
	val, ok, gErr := s.Get("tiger-bucket", "foo")
	assert.True(t, false == ok)
	assert.True(t, nil != gErr)
	assert.True(t, "" == val || 0 == len(val))
}

// Test_StoreInMemOpenSingleNode tests that a command can be applied to the log
// stored in RAM.
func Test_StoreInMemOpenSingleNode(t *testing.T) {
	tmpDir, _ := ioutil.TempDir("", "store_test")
	s := NewPeer(tmpDir, "127.0.0.1:0")
	s.RaftDir = tmpDir
	defer func() {
		_ = os.RemoveAll(tmpDir)
	}()
	if s == nil {
		t.Fatalf("failed to create store")
	}

	if err := s.Open("node0"); err != nil {
		t.Fatalf("failed to open store: %s", err)
	}

	// Simple way to ensure there is a leader.
	time.Sleep(3 * time.Second)

	if err := s.Set("tiger-bucket", "foo", "bar"); err != nil {
		t.Fatalf("failed to set key: %s", err.Error())
	}

	// Wait for committed log entry to be applied.
	time.Sleep(500 * time.Millisecond)
	value, _, err := s.Get("tiger-bucket", "foo")
	if err != nil {
		t.Fatalf("failed to get key: %s", err.Error())
	}
	if value != "bar" {
		t.Fatalf("key has wrong value: %s", value)
	}

	if err := s.Delete("tiger-bucket", "foo"); err != nil {
		t.Fatalf("failed to delete key: %s", err.Error())
	}

	// Wait for committed log entry to be applied.
	time.Sleep(500 * time.Millisecond)
	value, _, err = s.Get("tiger-bucket", "foo")
	if err != nil {
		t.Log("failed to get key", err.Error())
	}
	if value != "" {
		t.Fatalf("key should have no value, but: %s", value)
	}
}

// Test start a new raft cluster, and restart all
func TestStartAndRestartPeer(t *testing.T) {
	dir, _ := os.UserCacheDir()
	m := map[string]config{
		"peer01": {
			"peer01", "127.0.0.1:30001", filepath.Join(dir, "raft-badger-test/store_test1"),
		},
		"peer02": {
			"peer02", "127.0.0.1:30002", filepath.Join(dir, "raft-badger-test/store_test2"),
		},
		"peer03": {
			"peer03", "127.0.0.1:30003", filepath.Join(dir, "raft-badger-test/store_test3"),
		},
	}
	for _, v := range m {
		startPeer(m["peer01"].raftAddr, v.id, v.raftAddr, v.dataPath, t)
		defer func() {
			_ = os.RemoveAll(v.dataPath)
		}()
	}

	time.Sleep(3600 * time.Second)
}

func startPeer(leaderAddr, id, raftAddr, path string, t *testing.T) {
	s := NewPeer(path, raftAddr)

	if err := s.Open(id); err != nil {
		t.Fatalf("failed to open peer id %s %s", id, err)
	}

	time.Sleep(3 * time.Second)
	if err := s.Join(id, leaderAddr); err != nil && !errors.Is(err, raft.ErrNotLeader) {
		t.Fatalf("failed to join leader peer id %s, %s", id, err)
	}
	time.Sleep(3 * time.Second)

	go func() {
		for {
			val, _ := json.MarshalIndent(s.Stats(), "", "\t")
			log.Printf("Raft Peer %s: %s\n", id, path)
			log.Printf("Raft stats %s\n", string(val))

			time.Sleep(3 * time.Second)
		}
	}()
}
