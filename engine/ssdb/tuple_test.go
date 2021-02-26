package ssdb

import (
	"reflect"
	"testing"
)

func Test_Serialize_Deserialize_Tuple(t *testing.T) {
	tuple := Tuple{Data: []TupleData{
		{Typ: Int32, Int32Val: 99},
		{Typ: Char64, Char64Val: [64]byte{'a', 'b', 'c'}},
	}}

	s, err := SerializeTuple(tuple)
	if err != nil {
		t.Errorf("error unexpected: %s", err)
	}

	nt, err := DeserializeTuple(s)
	if err != nil {
		t.Errorf("error unexpected: %s", err)
	}

	if len(nt.Data) != 2 {
		t.Errorf("unexpected length: %d", len(nt.Data))
	}

	if !reflect.DeepEqual(nt.Data[0], TupleData{Typ: Int32, Int32Val: 99}) {
		t.Errorf("unexpected Data[0]: %v", nt.Data[0])
	}

	if !reflect.DeepEqual(nt.Data[1], TupleData{Typ: Char64, Char64Val: [64]byte{'a', 'b', 'c'}}) {
		t.Errorf("unexpected Data[1]: %v", nt.Data[1])
	}
}
