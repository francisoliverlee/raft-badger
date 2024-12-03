package raft_badger

import (
	"errors"
	"fmt"
	kvstore "github.com/gmqio/kv-store"
	"github.com/hashicorp/raft"
	"github.com/stretchr/testify/assert"
	"io/ioutil"
	"log"
	"os"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

const (
	TestBucket = "TestBucket"
)

var (
	counter = int32(100)
)

func restartPeer(peer *Peer, t *testing.T) *Peer {
	p := NewPeer(peer.Id, peer.RaftDir, peer.RaftBind)

	if p == nil {
		t.Fatalf("failed to create store")
	}
	if err := p.Open(); err != nil {
		t.Fatalf("failed to open store: %s", err)
	}

	time.Sleep(time.Second * 3)
	return p
}

func testStartSingle(id, ip string, raftPort int, t *testing.T) *Peer {
	newPort := int(atomic.AddInt32(&counter, 1)) + raftPort

	tmpDir, _ := ioutil.TempDir("", "store_test-"+id)
	add := fmt.Sprintf("%s:%d", ip, newPort)
	p := NewPeer(id, tmpDir, add)

	if p == nil {
		t.Fatalf("failed to create store")
	}
	if err := p.Open(); err != nil {
		t.Fatalf("failed to open store %s: %s", id, err)
	}
	if err := p.StartSingle(); err != nil {
		t.Fatalf("failed to StartSingle %s: %s", id, err)
	}

	time.Sleep(time.Second * 3)
	stats := p.Stats()
	log.Printf(toJsonStringTest(stats))
	if n, err := strconv.Atoi(stats["num_peers"]); err != nil {
		t.Fatalf("failed to open store %s, convert num_peers error %s", id, err)
	} else {
		if n != 0 {
			t.Fatalf("%s num_peers should be 0 , but %d", id, n)
		}
	}

	if stats["state"] != raft.Leader.String() {
		t.Fatalf("%s state should leader , but %s", id, stats["state"])
	}

	return p
}

func testStartFollower(id, ip string, raftPort int, leader *Peer, t *testing.T) *Peer {
	newPort := int(atomic.AddInt32(&counter, 1)) + raftPort
	tmpDir, _ := ioutil.TempDir("", "store_test-"+id)
	add := fmt.Sprintf("%s:%d", ip, newPort)
	p := NewPeer(id, tmpDir, add)

	if p == nil {
		t.Fatalf("failed to create store[%s]", id)
	}
	if err := p.Open(); err != nil {
		t.Fatalf("failed to open store[%s]: %s", id, err)
	}

	if err := leader.Join(id, add); err != nil {
		t.Fatalf("failed to join %s in leader. %s", id, err)
	}

	time.Sleep(time.Second * 3)
	stats := p.Stats()
	log.Printf(toJsonStringTest(stats))
	assert.True(t, stats["state"] == raft.Follower.String())

	return p
}

// Test_StartSingle start a single peer, start self-elect
func Test_StartSingle(t *testing.T) {
	leader := testStartSingle("node00", "127.0.0.1", 30001, t)
	defer func() {
		_ = os.RemoveAll(leader.RaftDir)
	}()
}

// Test_StartSingleAgain start a single peer, start self-elect. restart it
func Test_StartSingleAgain(t *testing.T) {
	leader := testStartSingle("node00", "127.0.0.1", 30001, t)

	if err := leader.Close(); err != nil {
		t.Fatalf("failed to close leader: %s", err)
	}
	t.Log("[Test_StartSingleAgain] close it done")

	if err := leader.Open(); err != nil {
		t.Fatalf("failed to re-open leader: %s", err)
	}
	t.Log("[Test_StartSingleAgain] open an-old-peer done")

	if err := leader.StartSingle(); err != nil {
		t.Fatalf("failed to re-StartSingle leader: %s", err)
	}
	t.Log("[Test_StartSingleAgain] re-StartSingle an-old-peer done")

	time.Sleep(3 * time.Second)
	t.Log("[Test_StartSingleAgain] restart an-old-peer done")
	defer func() {
		_ = os.RemoveAll(leader.RaftDir)
	}()
}

// Test_StartSingles start 3 single peers, this is wrong to start raft clusters, when first start you should JOIN-A-LEADER
func Test_StartSingles1(t *testing.T) {
	// start 3 peers
	p1 := testStartSingle("node01", "127.0.0.1", 30001, t)
	defer func() {
		_ = os.RemoveAll(p1.RaftDir)
	}()
	p2 := testStartSingle("node02", "127.0.0.1", 40001, t)
	defer func() {
		_ = os.RemoveAll(p2.RaftDir)
	}()
	p3 := testStartSingle("node03", "127.0.0.1", 50001, t)
	defer func() {
		_ = os.RemoveAll(p2.RaftDir)
	}()

	// wait raft cluster update state
	time.Sleep(3 * time.Second)

	states := map[string]map[string]string{}
	{
		states[p1.Id] = p1.Stats()
		states[p2.Id] = p2.Stats()
		states[p3.Id] = p3.Stats()
	}

	leaderCount := 0
	FollowerCount := 0

	for id, m := range states {
		if m["state"] == raft.Leader.String() {
			leaderCount = leaderCount + 1
		} else if m["state"] == raft.Follower.String() {
			FollowerCount = FollowerCount + 1
		} else {
			t.Fatalf("peer[%s] state should be leader or follower but %s", id, m["state"])
		}
		log.Printf("id=%s, state=%s, num_peers=%s", id, m["state"], m["num_peers"])
	}
	log.Printf("leader should be 0, but %d, follower: %d", leaderCount, FollowerCount)
}

// Test_StartNewLeader2 start a leader, set, get
func Test_StartNewLeader_SetGet(t *testing.T) {
	leader := testStartSingle("node00", "127.0.0.1", 30001, t)
	defer func() {
		_ = os.RemoveAll(leader.RaftDir)
	}()

	key := "tiger-key"
	val := "tiger-value"
	if err := leader.Set(TestBucket, key, val); err != nil {
		t.Fatalf("failed to set key %s with value %s: %s", key, val, err)
	}

	if v, _, err := leader.Get(TestBucket, key); err != nil {
		t.Fatalf("failed to get key %s: %s", key, err)
	} else {
		assert.True(t, val == v)
	}
}

// Test_StartNewLeader3 start a leader, set, pget
func Test_StartNewLeader_SetPGet(t *testing.T) {
	leader := testStartSingle("node00", "127.0.0.1", 30001, t)
	defer func() {
		_ = os.RemoveAll(leader.RaftDir)
	}()

	key := "tiger-key"
	val := "tiger-value"
	if err := leader.Set(TestBucket, key, val); err != nil {
		t.Fatalf("failed to set key %s with value %s: %s", key, val, err)
	}

	if m, err := leader.PGet(TestBucket, []string{key}); err != nil {
		t.Fatalf("failed to pget key %s: %s", key, err)
	} else {
		assert.True(t, val == m[key])
	}
}

// Test_StartNewLeader3 start a leader, pset, get
func Test_StartNewLeader_PSetGet(t *testing.T) {
	leader := testStartSingle("node00", "127.0.0.1", 30001, t)
	defer func() {
		_ = os.RemoveAll(leader.RaftDir)
	}()

	key := "tiger-key"
	val := "tiger-value"
	if err := leader.PSet(TestBucket, map[string]string{
		key: val,
	}); err != nil {
		t.Fatalf("failed to set key %s with value %s: %s", key, val, err)
	}

	if v, _, err := leader.Get(TestBucket, key); err != nil {
		t.Fatalf("failed to get key %s: %s", key, err)
	} else {
		assert.True(t, val == v)
	}
}

// Test_StartNewLeader3 start a leader, pset, get
func Test_StartNewLeader_PSetPGet(t *testing.T) {
	leader := testStartSingle("node00", "127.0.0.1", 30001, t)
	defer func() {
		_ = os.RemoveAll(leader.RaftDir)
	}()

	key := "tiger-key"
	val := "tiger-value"
	if err := leader.PSet(TestBucket, map[string]string{
		key: val,
	}); err != nil {
		t.Fatalf("failed to set key %s with value %s: %s", key, val, err)
	}

	if m, err := leader.PGet(TestBucket, []string{key}); err != nil {
		t.Fatalf("failed to pget key %s: %s", key, err)
	} else {
		assert.True(t, val == m[key])
	}
}

// Test_StartNewLeaderAndNewFollowers start a cluster
func Test_StartNewLeaderAndNewFollowers(t *testing.T) {
	leader := testStartSingle("node01", "127.0.0.1", 30001, t)
	defer func() {
		_ = os.RemoveAll(leader.RaftDir)
	}()
	follower1 := testStartFollower("node02", "127.0.0.1", 40001, leader, t)
	defer func() {
		_ = os.RemoveAll(follower1.RaftDir)
	}()
	follower2 := testStartFollower("node03", "127.0.0.1", 50001, leader, t)
	defer func() {
		_ = os.RemoveAll(follower2.RaftDir)
	}()

	stats := leader.Stats()
	log.Printf("stats in leader: %s", toJsonStringTest(stats))
	if n, err := strconv.Atoi(stats["num_peers"]); err != nil {
		t.Fatalf("failed to open store, convert num_peers error %s", err)
	} else {
		assert.True(t, n == 2)
	}

	time.Sleep(3 * time.Second)

	stats1 := follower1.Stats()
	log.Printf("stats in leader follower 1: %s", toJsonStringTest(stats1))
	if n, err := strconv.Atoi(stats1["num_peers"]); err != nil {
		t.Fatalf("failed to open store, convert num_peers error %s", err)
	} else {
		assert.True(t, n == 2)
	}

	stats2 := follower1.Stats()
	log.Printf("stats in leader follower 2: %s", toJsonStringTest(stats2))
	if n, err := strconv.Atoi(stats2["num_peers"]); err != nil {
		t.Fatalf("failed to open store, convert num_peers error %s", err)
	} else {
		assert.True(t, n == 2)
	}
}

// Test_StartNewLeader3 start a cluster, set on leader, read on follower 1
func Test_StartNewLeader_SetLeaderGetFollower(t *testing.T) {
	leader := testStartSingle("node00", "127.0.0.1", 30001, t)
	defer func() {
		_ = os.RemoveAll(leader.RaftDir)
	}()

	follower1 := testStartFollower("node02", "127.0.0.1", 40001, leader, t)
	defer func() {
		_ = os.RemoveAll(follower1.RaftDir)
	}()

	key := "tiger-key"
	val := "tiger-value"
	if err := leader.Set(TestBucket, key, val); err != nil {
		t.Fatalf("failed to set key %s with value %s: %s", key, val, err)
	}

	start := time.Now()
	fmt.Printf("set on leader at " + start.String())

	wg := sync.WaitGroup{}
	wg.Add(1)
	go func() {
		for {
			v, found, _ := follower1.Get(TestBucket, key)
			if found {
				end := time.Now()
				log.Printf("get on follower at %s, diff=%d ms", end.String(), end.Sub(start).Milliseconds())
				assert.True(t, val == v)
				break
			} else {
				time.Sleep(10 * time.Millisecond)
				continue
			}
		}
		wg.Done()
	}()

	wg.Wait()
}

// TestStartAndShutdown start and shutdown
func TestStartAndShutdown(t *testing.T) {
	leader := testStartSingle("node01", "127.0.0.1", 30001, t)
	defer func() {
		_ = os.RemoveAll(leader.RaftDir)
	}()
	follower1 := testStartFollower("node02", "127.0.0.1", 40001, leader, t)
	defer func() {
		_ = os.RemoveAll(follower1.RaftDir)
	}()
	follower2 := testStartFollower("node03", "127.0.0.1", 50001, leader, t)
	defer func() {
		_ = os.RemoveAll(follower2.RaftDir)
	}()

	if err := leader.Close(); err != nil {
		t.Fatalf("shutdown leader error %v", err)
	}
	if err := follower1.Close(); err != nil {
		t.Fatalf("shutdown follower1 error %v", err)
	}
	if err := follower2.Close(); err != nil {
		t.Fatalf("shutdown follower2 error %v", err)
	}

	restartPeer(leader, t)
}

// TestStartAndRestart start a new cluster, stop all, start leader, start followers
func TestStartAndRestart1(t *testing.T) {
	leader := testStartSingle("node01", "127.0.0.1", 30001, t)
	follower1 := testStartFollower("node02", "127.0.0.1", 40001, leader, t)
	follower2 := testStartFollower("node03", "127.0.0.1", 50001, leader, t)
	defer func() {
		_ = os.RemoveAll(leader.RaftDir)
		_ = os.RemoveAll(follower1.RaftDir)
		_ = os.RemoveAll(follower2.RaftDir)

		if err := recover(); err != nil {
			log.Fatalf("recover error %v", err)
		}
	}()

	if err := leader.Close(); err != nil {
		t.Fatalf("shutdown leader error %v", err)
	}
	if err := follower1.Close(); err != nil {
		t.Fatalf("shutdown follower1 error %v", err)
	}
	if err := follower2.Close(); err != nil {
		t.Fatalf("shutdown follower2 error %v", err)
	}

	states := map[string]map[string]string{}
	l1 := restartPeer(leader, t)
	f1 := restartPeer(follower1, t)
	f2 := restartPeer(follower2, t)

	// wait raft cluster update state
	time.Sleep(3 * time.Second)

	{
		stats := l1.Stats()
		states[l1.Id] = stats
	}

	{
		stats := f1.Stats()
		states[f1.Id] = stats
	}

	{
		stats := f2.Stats()
		states[f2.Id] = stats
	}

	leaderCount := 0
	FollowerCount := 0
	for id, m := range states {
		if m["state"] == raft.Leader.String() {
			leaderCount = leaderCount + 1
		} else if m["state"] == raft.Follower.String() {
			FollowerCount = FollowerCount + 1
		} else {
			t.Fatalf("peer[%s] state should be leader or follower but %s", id, m["state"])
		}

		if n, err := strconv.Atoi(m["num_peers"]); err != nil {
			t.Fatalf("failed to convert num_peers[%s], %v", id, err)
		} else if n != 2 {
			t.Fatalf("peer[%s] count shoud be 2 but %d", id, n)
		}
	}

	if leaderCount != 1 {
		t.Fatalf("leader should be 1, but %d", leaderCount)
	}
	if FollowerCount != 2 {
		t.Fatalf("followers should be 2, but %d", FollowerCount)
	}
}

// TestStartAndRestart start a new cluster, stop all, start a follower, start leader, start a follower
func TestStartAndRestart2(t *testing.T) {
	leader := testStartSingle("node01", "127.0.0.1", 30001, t)
	follower1 := testStartFollower("node02", "127.0.0.1", 40001, leader, t)
	follower2 := testStartFollower("node03", "127.0.0.1", 50001, leader, t)
	defer func() {
		_ = os.RemoveAll(leader.RaftDir)
		_ = os.RemoveAll(follower1.RaftDir)
		_ = os.RemoveAll(follower2.RaftDir)

		if err := recover(); err != nil {
			log.Fatalf("recover error %v", err)
		}
	}()

	if err := leader.Close(); err != nil {
		t.Fatalf("shutdown leader error %v", err)
	}
	if err := follower1.Close(); err != nil {
		t.Fatalf("shutdown follower1 error %v", err)
	}
	if err := follower2.Close(); err != nil {
		t.Fatalf("shutdown follower2 error %v", err)
	}

	states := map[string]map[string]string{}
	f1 := restartPeer(follower1, t)
	l1 := restartPeer(leader, t)
	f2 := restartPeer(follower2, t)

	// wait raft cluster update state
	time.Sleep(3 * time.Second)

	{
		stats := l1.Stats()
		states[l1.Id] = stats
	}

	{
		stats := f1.Stats()
		states[f1.Id] = stats
	}

	{
		stats := f2.Stats()
		states[f2.Id] = stats
	}

	leaderCount := 0
	FollowerCount := 0
	for id, m := range states {
		if m["state"] == raft.Leader.String() {
			leaderCount = leaderCount + 1
		} else if m["state"] == raft.Follower.String() {
			FollowerCount = FollowerCount + 1
		} else {
			t.Fatalf("peer[%s] state should be leader or follower but %s", id, m["state"])
		}

		if n, err := strconv.Atoi(m["num_peers"]); err != nil {
			t.Fatalf("failed to convert num_peers[%s], %v", id, err)
		} else if n != 2 {
			t.Fatalf("peer[%s] count shoud be 2 but %d", id, n)
		}
	}

	if leaderCount != 1 {
		t.Fatalf("leader should be 1, but %d", leaderCount)
	}
	if FollowerCount != 2 {
		t.Fatalf("followers should be 2, but %d", FollowerCount)
	}
}

// TestStartAndRestart start a new cluster, stop all, start a follower, start a follower, start leader
func TestStartAndRestart3(t *testing.T) {
	leader := testStartSingle("node01", "127.0.0.1", 30001, t)
	follower1 := testStartFollower("node02", "127.0.0.1", 40001, leader, t)
	follower2 := testStartFollower("node03", "127.0.0.1", 50001, leader, t)
	defer func() {
		_ = os.RemoveAll(leader.RaftDir)
		_ = os.RemoveAll(follower1.RaftDir)
		_ = os.RemoveAll(follower2.RaftDir)

		if err := recover(); err != nil {
			log.Fatalf("recover error %v", err)
		}
	}()

	if err := leader.Close(); err != nil {
		t.Fatalf("shutdown leader error %v", err)
	}
	if err := follower1.Close(); err != nil {
		t.Fatalf("shutdown follower1 error %v", err)
	}
	if err := follower2.Close(); err != nil {
		t.Fatalf("shutdown follower2 error %v", err)
	}

	states := map[string]map[string]string{}
	f1 := restartPeer(follower1, t)
	f2 := restartPeer(follower2, t)
	l1 := restartPeer(leader, t)

	// wait raft cluster update state
	time.Sleep(3 * time.Second)

	{
		stats := l1.Stats()
		states[l1.Id] = stats
	}

	{
		stats := f1.Stats()
		states[f1.Id] = stats
	}

	{
		stats := f2.Stats()
		states[f2.Id] = stats
	}

	leaderCount := 0
	FollowerCount := 0
	for id, m := range states {
		if m["state"] == raft.Leader.String() {
			leaderCount = leaderCount + 1
		} else if m["state"] == raft.Follower.String() {
			FollowerCount = FollowerCount + 1
		} else {
			t.Fatalf("peer[%s] state should be leader or follower but %s", id, m["state"])
		}

		if n, err := strconv.Atoi(m["num_peers"]); err != nil {
			t.Fatalf("failed to convert num_peers[%s], %v", id, err)
		} else if n != 2 {
			t.Fatalf("peer[%s] count shoud be 2 but %d", id, n)
		}
	}

	if leaderCount != 1 {
		t.Fatalf("leader should be 1, but %d", leaderCount)
	}
	if FollowerCount != 2 {
		t.Fatalf("followers should be 2, but %d", FollowerCount)
	}
}

// Test_StartSingles start 3 single peers joining-a-leader, this is wrong to start raft clusters, when restart an old peer, not join-a-leader cause raft meta knows all peers' meta
func TestRestartForce(t *testing.T) {
	// start 3 peers
	leader := testStartSingle("node00", "127.0.0.1", 30001, t)
	follower1 := testStartFollower("node01", "127.0.0.1", 40001, leader, t)
	follower2 := testStartFollower("node02", "127.0.0.1", 50001, leader, t)
	defer func() {
		_ = os.RemoveAll(leader.RaftDir)
		_ = os.RemoveAll(follower1.RaftDir)
		_ = os.RemoveAll(follower2.RaftDir)
	}()
	leader.Close()
	follower1.Close()
	follower2.Close()
	time.Sleep(3 * time.Second)

	p1 := testStartSingle(leader.Id, "127.0.0.1", 30001, t)
	p2 := testStartFollower(follower1.Id, "127.0.0.1", 40001, p1, t)
	p3 := testStartFollower(follower2.Id, "127.0.0.1", 50001, p1, t)

	// wait raft cluster update state
	time.Sleep(3 * time.Second)

	states := map[string]map[string]string{}
	{
		states[p1.Id] = p1.Stats()
		states[p2.Id] = p2.Stats()
		states[p3.Id] = p3.Stats()
	}

	leaderCount := 0
	FollowerCount := 0

	for id, m := range states {
		if m["state"] == raft.Leader.String() {
			leaderCount = leaderCount + 1
		} else if m["state"] == raft.Follower.String() {
			FollowerCount = FollowerCount + 1
		} else {
			t.Fatalf("peer[%s] state should be leader or follower but %s", id, m["state"])
		}
		log.Printf("id=%s, state=%s, num_peers=%s", id, m["state"], m["num_peers"])
	}
	log.Printf("leader  %d, follower: %d", leaderCount, FollowerCount)
}

// test new peer join to sync data. start a cluster and set-key, stop a follower, add a whole new peer and get-key from it
func TestStartAndAddNewPeer(t *testing.T) {
	leader := testStartSingle("node01", "127.0.0.1", 30001, t)
	follower1 := testStartFollower("node02", "127.0.0.1", 40001, leader, t)
	follower2 := testStartFollower("node03", "127.0.0.1", 50001, leader, t)

	key := "tiger-key"
	val := "tiger-value"

	if err := leader.Set(TestBucket, key, val); err != nil {
		t.Fatalf("set key value error %s, %v", leader.Id, err)
	}
	defer func() {
		_ = os.RemoveAll(leader.RaftDir)
		_ = os.RemoveAll(follower1.RaftDir)
		_ = os.RemoveAll(follower2.RaftDir)

		if err := recover(); err != nil {
			log.Fatalf("recover error %v", err)
		}
	}()

	// stop follower
	if err := follower1.Close(); err != nil {
		t.Fatalf("close follower 1 error %v", err)
	}

	// start a new follower
	follower1New := testStartFollower("node02New", "127.0.0.1", 60001, leader, t)
	states := map[string]map[string]string{}

	// wait raft cluster update state
	time.Sleep(10 * time.Second)

	{
		stats := leader.Stats()
		states[leader.Id] = stats
	}

	{
		stats := follower2.Stats()
		states[follower2.Id] = stats
	}

	{
		stats := follower1New.Stats()
		states[follower1New.Id] = stats
	}

	leaderCount := 0
	FollowerCount := 0
	for id, m := range states {
		if m["state"] == raft.Leader.String() {
			leaderCount = leaderCount + 1
		} else if m["state"] == raft.Follower.String() {
			FollowerCount = FollowerCount + 1
		} else {
			t.Fatalf("peer[%s] state should be leader or follower but %s", id, m["state"])
		}

		if n, err := strconv.Atoi(m["num_peers"]); err != nil {
			t.Fatalf("failed to convert num_peers[%s], %v", id, err)
		} else if n != 3 {
			t.Fatalf("peer[%s] count shoud be 3 but %d", id, n)
		}
	}

	if leaderCount != 1 {
		t.Fatalf("leader should be 1, but %d", leaderCount)
	}
	if FollowerCount != 2 {
		t.Fatalf("followers should be 2, but %d", FollowerCount)
	}

	// check leader key sync
	if v, _, err := follower1New.Get(TestBucket, key); err != nil {
		t.Fatalf("read from new peer error %v", err)
	} else if v != val {
		t.Fatalf("val from new peer[%s] sync-from-leader[%s]", v, val)
	}
}

// test new peer join to sync data. start a cluster and set-key, remove a follower, add a whole new peer and get-key from it
func TestStartAndRemoveAddNewPeer(t *testing.T) {
	leader := testStartSingle("node01", "127.0.0.1", 30001, t)
	follower1 := testStartFollower("node02", "127.0.0.1", 40001, leader, t)
	follower2 := testStartFollower("node03", "127.0.0.1", 50001, leader, t)

	key := "tiger-key"
	val := "tiger-value"

	if err := leader.Set(TestBucket, key, val); err != nil {
		t.Fatalf("set key value error %s, %v", leader.Id, err)
	}
	defer func() {
		_ = os.RemoveAll(leader.RaftDir)
		_ = os.RemoveAll(follower1.RaftDir)
		_ = os.RemoveAll(follower2.RaftDir)

		if err := recover(); err != nil {
			log.Fatalf("recover error %v", err)
		}
	}()

	// stop follower
	if err := follower1.Close(); err != nil {
		t.Fatalf("close follower 1 error %v", err)
	}

	// remove follower 1
	if err := leader.Remove(follower1.Id, follower1.RaftBind); err != nil {
		t.Fatalf("remove follower 1 error %v", err)
	}
	// wait raft cluster update state
	time.Sleep(5 * time.Second)

	// start a new follower
	follower1New := testStartFollower("node02New", "127.0.0.1", 60001, leader, t)
	states := map[string]map[string]string{}

	// wait raft cluster update state
	time.Sleep(5 * time.Second)

	{
		stats := leader.Stats()
		states[leader.Id] = stats
	}

	{
		stats := follower2.Stats()
		states[follower2.Id] = stats
	}

	{
		stats := follower1New.Stats()
		states[follower1New.Id] = stats
	}

	leaderCount := 0
	FollowerCount := 0
	for id, m := range states {
		if m["state"] == raft.Leader.String() {
			leaderCount = leaderCount + 1
		} else if m["state"] == raft.Follower.String() {
			FollowerCount = FollowerCount + 1
		} else {
			t.Fatalf("peer[%s] state should be leader or follower but %s", id, m["state"])
		}

		if n, err := strconv.Atoi(m["num_peers"]); err != nil {
			t.Fatalf("failed to convert num_peers[%s], %v", id, err)
		} else if n != 3 {
			t.Fatalf("peer[%s] count shoud be 3 but %d", id, n)
		}
	}

	if leaderCount != 1 {
		t.Fatalf("leader should be 1, but %d", leaderCount)
	}
	if FollowerCount != 2 {
		t.Fatalf("followers should be 2, but %d", FollowerCount)
	}

	// check leader key sync
	if v, _, err := follower1New.Get(TestBucket, key); err != nil {
		t.Fatalf("read from new peer error %v", err)
	} else if v != val {
		t.Fatalf("val from new peer[%s] sync-from-leader[%s]", v, val)
	}
}

// test restart peer join to sync data. start a cluster and set-key, stop a follower, set new key , start follower, check new key
func TestSetGetOnRestartPeer(t *testing.T) {
	leader := testStartSingle("node01", "127.0.0.1", 30001, t)
	follower1 := testStartFollower("node02", "127.0.0.1", 40001, leader, t)
	follower2 := testStartFollower("node03", "127.0.0.1", 50001, leader, t)

	key := "tiger-key"
	val := "tiger-value"

	if err := leader.Set(TestBucket, key, val); err != nil {
		t.Fatalf("set key value error %s, %v", leader.Id, err)
	}
	defer func() {
		_ = os.RemoveAll(leader.RaftDir)
		_ = os.RemoveAll(follower1.RaftDir)
		_ = os.RemoveAll(follower2.RaftDir)

		if err := recover(); err != nil {
			log.Fatalf("recover error %v", err)
		}
	}()

	if err := follower1.Close(); err != nil {
		t.Fatalf("shutdown follower1 error %v", err)
	}
	states := map[string]map[string]string{}

	// wait raft cluster update state
	time.Sleep(3 * time.Second)

	key1 := "tiger-key-1"
	val1 := "tiger-value-1"

	if err := leader.Set(TestBucket, key1, val1); err != nil {
		t.Fatalf("set key value error %s, %v", leader.Id, err)
	}

	follower1New := restartPeer(follower1, t)
	time.Sleep(3 * time.Second)

	{
		stats := leader.Stats()
		states[leader.Id] = stats
	}

	{
		stats := follower2.Stats()
		states[follower2.Id] = stats
	}

	{
		stats := follower1New.Stats()
		states[follower1New.Id] = stats
	}

	leaderCount := 0
	FollowerCount := 0
	for id, m := range states {
		if m["state"] == raft.Leader.String() {
			leaderCount = leaderCount + 1
		} else if m["state"] == raft.Follower.String() {
			FollowerCount = FollowerCount + 1
		} else {
			t.Fatalf("peer[%s] state should be leader or follower but %s", id, m["state"])
		}

		if n, err := strconv.Atoi(m["num_peers"]); err != nil {
			t.Fatalf("failed to convert num_peers[%s], %v", id, err)
		} else if n != 2 {
			t.Fatalf("peer[%s] count shoud be 2 but %d", id, n)
		}
	}

	if leaderCount != 1 {
		t.Fatalf("leader should be 1, but %d", leaderCount)
	}
	if FollowerCount != 2 {
		t.Fatalf("followers should be 2, but %d", FollowerCount)
	}

	// check leader key sync
	if v, _, err := follower1New.Get(TestBucket, key); err != nil {
		t.Fatalf("read from old peer error %v", err)
	} else if v != val {
		t.Fatalf("val from old peer[%s] sync-from-leader[%s]", v, val)
	}

	if v, _, err := follower1New.Get(TestBucket, key1); err != nil {
		t.Fatalf("read new key from old peer error %v", err)
	} else if v != val1 {
		t.Fatalf("val new key from old peer[%s] sync-from-leader[%s]", v, val)
	}
}

// test set, delete, get
func TestDelete(t *testing.T) {
	leader := testStartSingle("node00", "127.0.0.1", 30001, t)
	defer func() {
		_ = os.RemoveAll(leader.RaftDir)
	}()

	key := "tiger-key"
	val := "tiger-value"
	if err := leader.Set(TestBucket, key, val); err != nil {
		t.Fatalf("failed to set key %s with value %s: %s", key, val, err)
	}

	if v, _, err := leader.Get(TestBucket, key); err != nil {
		t.Fatalf("failed to get key %s: %s", key, err)
	} else {
		assert.True(t, val == v)
	}

	if err := leader.Delete(TestBucket, key); err != nil {
		t.Fatalf("failed to delete key %s with value %s: %s", key, val, err)
	}

	if _, f, err := leader.Get(TestBucket, key); !errors.Is(err, kvstore.KeyNotFoundError) {
		t.Fatalf("failed to get key %s. should be a delted key but error %s", key, err)
	} else {
		assert.True(t, f == false)
	}

}

// test set, p delete, get
func TestDeleteKeys(t *testing.T) {
	leader := testStartSingle("node00", "127.0.0.1", 30001, t)
	defer func() {
		_ = os.RemoveAll(leader.RaftDir)
	}()

	m := map[string]string{}
	key := "tiger-key"
	val := "tiger-value"
	for i := 0; i < 100; i++ {
		m[key+strconv.Itoa(i)] = val + strconv.Itoa(i)
	}

	if err := leader.PSet(TestBucket, m); err != nil {
		t.Fatalf("failed to p set keya %v", err)
	}

	for k, val := range m {
		if v, _, err := leader.Get(TestBucket, k); err != nil {
			t.Fatalf("failed to get key %s: %s", k, err)
		} else {
			assert.True(t, val == v)
		}
	}

	if err := leader.PDelete(TestBucket, MKeys(m)); err != nil {
		t.Fatalf("failed to delete keys %v", err)
	}

	for k, _ := range m {
		if _, f, err := leader.Get(TestBucket, k); !errors.Is(err, kvstore.KeyNotFoundError) {
			t.Fatalf("failed to get keys %s. should be a delted key but error %s", k, err)
		} else {
			assert.True(t, f == false)
		}
	}
}

// test keys
func TestKeys(t *testing.T) {
	leader := testStartSingle("node00", "127.0.0.1", 30001, t)
	defer func() {
		_ = os.RemoveAll(leader.RaftDir)
	}()

	// gen dataå
	m := map[string]string{}
	key := "tiger-key"
	val := "tiger-value"
	for i := 0; i < 100; i++ {
		m[key+strconv.Itoa(i)] = val + strconv.Itoa(i)
	}

	// pset data
	if err := leader.PSet(TestBucket, m); err != nil {
		t.Fatalf("failed to p set keys %v", err)
	}

	// scan key prefix
	newBucket := leader.buildKey(TestBucket, key)
	if mnew, err := leader.Keys(newBucket); err != nil {
		t.Fatalf("failed to p set keya %v", err)
	} else {
		fmt.Println(mnew)
	}

}
