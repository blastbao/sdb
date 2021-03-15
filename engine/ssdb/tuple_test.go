package ssdb

import (
	"reflect"
	"testing"
)

func Test_Serialize_Deserialize_Tuple(t *testing.T) {
	tuple := Tuple{Data: []TupleData{
		{Typ: Int32, Int32Val: 99},
		{Typ: Byte64, Byte64Val: [64]byte{'a', 'b', 'c'}},
	}}

	s := SerializeTuple(tuple)
	expected := make([]byte, 4+4+4+64)
	copy(expected[0:4], []byte{0, 0, 0, 1})   // Type Int32
	copy(expected[4:8], []byte{0, 0, 0, 99})  // Value 99
	copy(expected[8:12], []byte{0, 0, 0, 2})  // Type Byte64
	copy(expected[12:15], []byte{97, 98, 99}) // a, b, c
	// rest bytes are zero-initialized

	if !reflect.DeepEqual(s, expected) {
		t.Errorf("unexpected serialized bytes: \n%v, expected: \n%v", s, expected)
	}

	nt := DeserializeTuple(s)
	if len(nt.Data) != 2 {
		t.Errorf("unexpected length: %d", len(nt.Data))
	}

	if !reflect.DeepEqual(nt.Data[0], TupleData{Typ: Int32, Int32Val: 99}) {
		t.Errorf("unexpected Data[0]: %v", nt.Data[0])
	}

	if !reflect.DeepEqual(nt.Data[1], TupleData{Typ: Byte64, Byte64Val: [64]byte{'a', 'b', 'c'}}) {
		t.Errorf("unexpected Data[1]: %v", nt.Data[1])
	}
}
