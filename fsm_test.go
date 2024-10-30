package raft_badger

import (
	"errors"
	"fmt"
	"log"
	"testing"
)

var (
	sinkError  = false
	writeError = errors.New("TestSink write error")
)

type TestSink struct {
}

func (t TestSink) Write(p []byte) (n int, err error) {
	if sinkError {
		return 0, writeError
	}
	fmt.Println(string(p))
	return len(p), nil
}

func (t TestSink) Close() error {
	log.Printf("TestSink close")
	return nil
}

func (t TestSink) ID() string {
	return "TestSink"
}

func (t TestSink) Cancel() error {
	log.Printf("TestSink Cancel")
	return nil
}

// persist normal
func Test_fsmSnapshot_Persist1(t *testing.T) {
	s := newFsmSnapshot()
	s.store["a"] = "a"
	s.store["b"] = "b"

	if err := s.Persist(TestSink{}); err != nil {
		t.Fatalf("persist error %v", err)
	}
}

// persist error, return Cancel result
func Test_fsmSnapshot_Persist2(t *testing.T) {
	sinkError = true
	s := newFsmSnapshot()
	s.store["a"] = "a"
	s.store["b"] = "b"

	if err := s.Persist(TestSink{}); err != nil {
		t.Fatalf("persist error should be nil but %v", err)
	}
	// check log output "TestSink Cancel"
}
