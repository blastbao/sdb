package ssdb

import (
	"reflect"
	"testing"

	"github.com/dty1er/sdb/testutil"
)

func TestPageHeader_encode(t *testing.T) {
	ph := &pageHeader{
		id:          42,
		tuplesCount: 3,
		slots: []slot{
			{offset: 0, length: 10},
			{offset: 10, length: 25},
			{offset: 35, length: 50},
		},
	}

	encoded := ph.encode()

	testutil.MustEqual(
		t,
		encoded,
		[]byte{
			0, 0, 0, 42, // page id (4 byte)
			0, 3, // tuples count (2 byte)
			0, 0, 0, 10, // slot[0]: offset (2 byte), length (2 byte),
			0, 10, 0, 25, // slot[1]
			0, 35, 0, 50, // slot[2]
		},
	)
}

func TestNewPage(t *testing.T) {
	page := NewPage(42)
	expected := [16 * 1024]byte{}
	copy(expected[0:4], []byte{0, 0, 0, 42})
	if !reflect.DeepEqual(page.bs, expected) {
		t.Errorf("unexpected empty page: %v, expected: %v", page, expected)
	}

	id := page.GetID()

	if id != PageID(42) {
		t.Errorf("unexpected id: %d", id)
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

	// make sure err is responded when the page has no space
	tuple := Tuple{
		Data: []TupleData{
			{Typ: Int32, Int32Val: 96},
			{Typ: Byte64, Byte64Val: [64]byte{'a', 'b', 'c'}},
		},
	}

	tupleSize := len(SerializeTuple(tuple))
	// a page can contains $max tuples
	// -4 because a page always has 4 byte ID
	// -2 because a page always has 2 byte tuplesCount
	// +4 because a slot is 4 byte
	max := (PageSize - 4 - 2) / (tupleSize + 4)

	page = NewPage(50)
	// append $max tuples in the page.
	// Error should not happen.
	for i := 0; i < max; i++ {
		if err := page.AppendTuple(tuple); err != nil {
			t.Errorf("unexpected error: %v", err)
		}
	}

	// because the page already contains $max tuples,
	// no available space error must happen.
	if err := page.AppendTuple(tuple); err == nil {
		t.Errorf("error must happen")
	}
}
