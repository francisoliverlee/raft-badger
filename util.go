package raft_badger

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"github.com/hashicorp/raft"
	"log"
	"strings"
)

func encodeKey(prefix, userKey []byte) []byte {
	buf := bytes.NewBuffer(make([]byte, len(prefix)+len(userKey)))
	_ = binary.Write(buf, binary.BigEndian, prefix)
	_ = binary.Write(buf, binary.BigEndian, userKey)
	return buf.Bytes()
}

func encodeRaftLogKey(idx uint64) []byte {
	buf := bytes.NewBuffer(make([]byte, len(LogBucket)+8))
	_ = binary.Write(buf, binary.BigEndian, LogBucket)
	_ = binary.Write(buf, binary.BigEndian, idx)

	return buf.Bytes()
}

func encodeRaftLog(l *raft.Log) ([]byte, error) {
	return json.Marshal(*l)
}

func decodeRaftLog(val []byte, l *raft.Log) error {
	return json.Unmarshal(val, &l)
}

func bytesToUint64(b []byte) uint64 {
	return binary.BigEndian.Uint64(b)
}

func uint64ToBytes(u uint64) []byte {
	buf := make([]byte, 8)
	binary.BigEndian.PutUint64(buf, u)
	return buf
}

func PrintMapDiff(baseKv, newKv map[string]string) {
	bCount := 0
	nCount := 0

	if baseKv != nil {
		bCount = len(baseKv)
	}
	if newKv != nil {
		nCount = len(newKv)
	}
	log.Printf("baseKv count %d, newKv count %d", bCount, nCount)
	if bCount == 0 || nCount == 0 {
		return
	}

	var diff1 []string
	for k, _ := range baseKv {
		_, exist := newKv[k]
		if !exist {
			diff1 = append(diff1, k)
		}
	}

	var diff2 []string
	for k, _ := range newKv {
		_, exist := baseKv[k]
		if !exist {
			diff2 = append(diff2, k)
		}
	}

	log.Printf("newKv - baseKv: diff keys %s", strings.Join(diff1, ","))
	log.Printf("baseKv - newKv: diff keys %s", strings.Join(diff2, ","))
}
