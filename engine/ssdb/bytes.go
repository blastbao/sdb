package ssdb

import "encoding/binary"

// ssdb uses BigEndian as its byteOrder in their byte representation
var byteOrder = binary.BigEndian

func bytesToUint16(bs []byte) uint16 {
	return byteOrder.Uint16(bs)
}

func bytesToUint32(bs []byte) uint32 {
	return byteOrder.Uint32(bs)
}

func putUint16OnBytes(bs []byte, v uint16) {
	byteOrder.PutUint16(bs, v)
}

func putUint32OnBytes(bs []byte, v uint32) {
	byteOrder.PutUint32(bs, v)
}

func putUint64OnBytes(bs []byte, v uint64) {
	byteOrder.PutUint64(bs, v)
}
