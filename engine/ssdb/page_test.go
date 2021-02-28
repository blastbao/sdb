package ssdb

import (
	"reflect"
	"testing"
)

func Test_Serialize_Deserialize_Page(t *testing.T) {
	tuples := []Tuple{
		{Data: []TupleData{{Typ: Int32, Int32Val: 96}, {Typ: Char64, Char64Val: [64]byte{'a', 'b', 'c'}}}},
		{Data: []TupleData{{Typ: Int32, Int32Val: 97}, {Typ: Char64, Char64Val: [64]byte{'d', 'e', 'f'}}}},
		{Data: []TupleData{{Typ: Char64, Char64Val: [64]byte{'g', 'h', 'i', 'j', 'k'}}, {Typ: Int32, Int32Val: 98}}},
		{Data: []TupleData{{Typ: Char64, Char64Val: [64]byte{'l', 'm', 'n', 'o', 'p'}}, {Typ: Int32, Int32Val: 99}}},
	}

	page := &Page{Tuples: tuples}

	s, err := SerializePage(page)
	if err != nil {
		t.Errorf("error unexpected: %s", err)
	}

	np, err := DeserializePage(s)
	if err != nil {
		t.Errorf("error unexpected: %s", err)
	}

	if len(np.Tuples) != 4 {
		t.Errorf("unexpected length: %d", len(np.Tuples))
	}

	for i, given := range np.Tuples {
		if !reflect.DeepEqual(given, tuples[i]) {
			t.Errorf("unexpected Data[i]: %v", given)
		}
	}

	if np.Header.TupleCount != 4 {
		t.Errorf("unexpected TupleCount: %d", np.Header.TupleCount)
	}
}
