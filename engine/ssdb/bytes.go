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
