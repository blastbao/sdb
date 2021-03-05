package ssdb

import "encoding/binary"

// ssdb uses BigEndian as its byteOrder in their byte representation
var byteOrder = binary.BigEndian

func bytesToUint32(bs []byte) uint32 {
	return byteOrder.Uint32(bs)
}
