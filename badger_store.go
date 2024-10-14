package raft_badger

import (
	"github.com/dgraph-io/badger/v4"
	"log"
	"os"
)

const (
	debugFlag = "raftBadgerDebug"
)

var (
	dbLogPrefixKey  = []byte("_rbl_")    // raft badger log
	dbConfPrefixKey = []byte("_rbl_cf_") // raft badger log config

	fsmKeyPrefix = []byte("_fmsl_") // raft badger log config

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

func (b *BadgerStore) Sync() error {
	return b.db.Sync()
}

func (b *BadgerStore) Stats() Stats {
	return Stats{}
}

func l(format string, v ...any) {
	if debug(debugVal) {
		log.Printf(format, v...)
	}
}

func debug(debugVal string) bool {
	return debugVal != ""
}
