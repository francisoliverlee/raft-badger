package raft_badger

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"github.com/hashicorp/raft"
)

func encodeConfigKey(prefix, userKey []byte) []byte {
	buf := bytes.NewBuffer(make([]byte, len(prefix)+len(userKey)))
	_ = binary.Write(buf, binary.BigEndian, prefix)
	_ = binary.Write(buf, binary.BigEndian, userKey)
	return buf.Bytes()
}

func encodeRaftLogKey(idx uint64) []byte {
	buf := bytes.NewBuffer(make([]byte, len(dbLogPrefixKey)+8))
	_ = binary.Write(buf, binary.BigEndian, dbLogPrefixKey)
	_ = binary.Write(buf, binary.BigEndian, idx)

	return buf.Bytes()
}

func encodeRaftLog(l *raft.Log) ([]byte, error) {
	return json.Marshal(*l)
}

func decodeRaftLog(val []byte) (*raft.Log, error) {
	var l raft.Log
	err := json.Unmarshal(val, &l)
	return &l, err
}

func bytesToUint64(b []byte) uint64 {
	return binary.BigEndian.Uint64(b)
}

func uint64ToBytes(u uint64) []byte {
	buf := make([]byte, 8)
	binary.BigEndian.PutUint64(buf, u)
	return buf
}

func Debug(debugVal string) bool {
	return debugVal != ""
}
