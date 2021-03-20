package ssdb

import (
	"reflect"
	"testing"
)

func TestNewPage(t *testing.T) {
	page := NewPage(42)
	expected := [16 * 1024]byte{}
	copy(expected[0:4], []byte{0, 0, 0, 42})
	if !reflect.DeepEqual(page.bs, expected) {
		t.Errorf("unexpected empty page: %v, expected: %v", page, expected)
	}
}

func TestPage_AppendTuple(t *testing.T) {
	tuples := []Tuple{
		{
			Data: []TupleData{
				{Typ: Int32, Int32Val: 96},
				{Typ: Byte64, Byte64Val: [64]byte{'a', 'b', 'c'}},
			},
		},
		{
			Data: []TupleData{
				{Typ: Int32, Int32Val: 97},
				{Typ: Byte64, Byte64Val: [64]byte{'d', 'e', 'f'}},
			},
		},
		{
			Data: []TupleData{
				{Typ: Byte64, Byte64Val: [64]byte{'g', 'h', 'i', 'j', 'k'}},
				{Typ: Int32, Int32Val: 98},
			},
		},
		{
			Data: []TupleData{
				{Typ: Byte64, Byte64Val: [64]byte{'l', 'm', 'n', 'o', 'p'}},
				{Typ: Int32, Int32Val: 99},
			},
		},
	}
	page := NewPage(42)

	for _, tuple := range tuples {
		if err := page.AppendTuple(tuple); err != nil {
			t.Errorf("unexpected error: %s", err)
		}
	}

	header := page.decodeHeader()
	if header.id != 42 {
		t.Errorf("unexpected page id: %v", header.id)
	}

	if header.tuplesCount != 4 {
		t.Errorf("unexpected tuplesCount: %v", header.tuplesCount)
	}

	if len(header.slots) != 4 {
		t.Errorf("unexpected slots: %v", header.slots)
	}

	for i := 0; i < int(header.tuplesCount); i++ {
		slot := header.slots[i]
		tp := DeserializeTuple(page.bs[slot.offset : slot.offset+slot.length])
		if !reflect.DeepEqual(tp, tuples[i]) {
			t.Errorf("unexpected deserialized tuple: %v", t)
		}
	}
}
