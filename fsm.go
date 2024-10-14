package raft_badger

import (
	"encoding/json"
	"fmt"
	"github.com/dgraph-io/badger/v4"
	"github.com/hashicorp/raft"
	"io"
	"log"
)

type fsmCommand struct {
	Op    string `json:"op,omitempty"`
	Key   string `json:"key,omitempty"`
	Value string `json:"value,omitempty"`
}

type fsmSnapshot struct {
	store map[string]string
}

type fsm BadgerStore

func (f *fsm) Apply(l *raft.Log) interface{} {
	var c fsmCommand
	if err := json.Unmarshal(l.Data, &c); err != nil {
		panic(fmt.Sprintf("failed to do apply unmarshal, error: %s", err.Error()))
	}

	key := encodeKey(fsmKeyPrefix, []byte(c.Key))

	switch c.Op {
	case "set":
		if err := f.db.Update(func(txn *badger.Txn) error {
			return txn.Set(key, []byte(c.Value))
		}); err != nil {
			panic(fmt.Sprintf("failed to apply set. %s=%s %s", string(key), c.Value, err.Error()))
		}
	case "delete":
		if err := f.db.Update(func(txn *badger.Txn) error {
			return txn.Delete(key)
		}); err != nil {
			panic(fmt.Sprintf("failed to apply delete. %s=%s %s", string(key), c.Value, err.Error()))
		}
	default:
		panic(fmt.Sprintf("failed to do apply %s for unknown . %s=%s", c.Op, string(key), c.Value))
	}

	return nil
}

func (f *fsm) Snapshot() (raft.FSMSnapshot, error) {
	pattern := fsmKeyPrefix
	kvMap := map[string]string{}

	err := f.db.View(func(txn *badger.Txn) error {
		it := txn.NewIterator(badger.DefaultIteratorOptions)
		defer it.Close()
		for it.Seek(pattern); it.ValidForPrefix(pattern); it.Next() {
			item := it.Item()
			if item.IsDeletedOrExpired() {
				continue
			}

			if vErr := item.Value(func(val []byte) error {
				kvMap[string(item.Key())] = string(val)
				return nil
			}); vErr != nil {
				log.Fatalf("read value error, key=%s. %v", string(item.Key()), vErr)
			}
		}
		return nil
	})

	return &fsmSnapshot{store: kvMap}, err
}

func (f *fsm) Restore(rc io.ReadCloser) error {
	pattern := fsmKeyPrefix
	newKv := make(map[string]string)
	if err := json.NewDecoder(rc).Decode(&newKv); err != nil {
		return err
	}

	s, sEr := f.Snapshot()
	if sEr != nil {
		log.Printf("Restore warn, get snapshot error. %s.  would replace all kv", sEr)
	}
	var fsmS = s.(*fsmSnapshot)
	PrintMapDiff(fsmS.store, newKv)

	// NOTICE : big newKv would spend lots of time
	return f.db.Update(func(txn *badger.Txn) error {
		for k, v := range newKv {
			key := encodeKey(pattern, []byte(k))
			if err := txn.Set(key, []byte(v)); err != nil {
				return err
			}
		}
		return nil
	})
}

func (f *fsmSnapshot) Persist(sink raft.SnapshotSink) error {
	err := func() error {
		// Encode data.
		b, err := json.Marshal(f.store)
		if err != nil {
			return err
		}

		// Write data to sink.
		if _, err := sink.Write(b); err != nil {
			return err
		}

		// Close the sink.
		return sink.Close()
	}()

	if err != nil {
		sink.Cancel()
	}

	return err
}

func (f *fsmSnapshot) Release() {

}
