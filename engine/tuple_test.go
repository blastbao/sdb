package engine

import (
	"testing"

	"github.com/dty1er/sdb/testutil"
)

func Test_Serialize_Deserialize_Tuple(t *testing.T) {
	tuple := &Tuple{Data: []*TupleData{
		{Typ: Int32, Int32Val: 99},
		{Typ: Byte64, Byte64Val: [64]byte{'a', 'b', 'c'}},
	}}

	s := SerializeTuple(tuple)
	expected := make([]byte, 4+4+4+64)
	copy(expected[0:4], []byte{0, 0, 0, 1})   // Type Int32
	copy(expected[4:8], []byte{0, 0, 0, 99})  // Value 99
	copy(expected[8:12], []byte{0, 0, 0, 3})  // Type Byte64
	copy(expected[12:15], []byte{97, 98, 99}) // a, b, c
	// rest bytes are zero-initialized

	testutil.MustEqual(t, s, expected)

	nt := DeserializeTuple(s)
	testutil.MustEqual(t, len(nt.Data), 2)
	testutil.MustEqual(t, nt.Data[0], &TupleData{Typ: Int32, Int32Val: 99})
	testutil.MustEqual(t, nt.Data[1], &TupleData{Typ: Byte64, Byte64Val: [64]byte{'a', 'b', 'c'}})
}
