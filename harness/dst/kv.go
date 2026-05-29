package dst

import "encoding/binary"

// Write proposal payload layout (17 bytes):
//   [0]    tag = opWriteTag
//   [1:9]  opID  (BE uint64) — unique per (cluster, op) so we can match
//          a committed entry back to the issuing client
//   [9:17] value (BE uint64) — the value being written to the register
//
// Non-write entries (e.g. EntryConfChange) and any malformed entries are
// silently ignored by the state machine.

const opWriteTag byte = 0x01

func EncodeWrite(opID, value uint64) []byte {
	buf := make([]byte, 17)
	buf[0] = opWriteTag
	binary.BigEndian.PutUint64(buf[1:9], opID)
	binary.BigEndian.PutUint64(buf[9:17], value)
	return buf
}

func DecodeWrite(data []byte) (opID, value uint64, ok bool) {
	if len(data) != 17 || data[0] != opWriteTag {
		return 0, 0, false
	}
	return binary.BigEndian.Uint64(data[1:9]),
		binary.BigEndian.Uint64(data[9:17]),
		true
}
