package engine

import (
	"testing"

	"github.com/dty1er/sdb/testutil"
)

func TestPageHeader_encode(t *testing.T) {
	ph := &pageHeader{
		id:          42,
		tuplesCount: 3,
		slots: []*slot{
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

func TestInitPage(t *testing.T) {
	page := InitPage(42)
	expected := [16 * 1024]byte{}
	copy(expected[0:4], []byte{0, 0, 0, 42})
	testutil.MustEqual(t, page.bs, expected)

	id := page.GetID()
	testutil.MustEqual(t, id, PageID(42))
}

func TestPage_AppendTuple(t *testing.T) {
	tuples := []*Tuple{
		{
			Data: []*TupleData{
				{Typ: Int32, Int32Val: 96},
				{Typ: Byte64, Byte64Val: [64]byte{'a', 'b', 'c'}},
			},
		},
		{
			Data: []*TupleData{
				{Typ: Int32, Int32Val: 97},
				{Typ: Byte64, Byte64Val: [64]byte{'d', 'e', 'f'}},
			},
		},
		{
			Data: []*TupleData{
				{Typ: Byte64, Byte64Val: [64]byte{'g', 'h', 'i', 'j', 'k'}},
				{Typ: Int32, Int32Val: 98},
			},
		},
		{
			Data: []*TupleData{
				{Typ: Byte64, Byte64Val: [64]byte{'l', 'm', 'n', 'o', 'p'}},
				{Typ: Int32, Int32Val: 99},
			},
		},
	}
	page := InitPage(42)

	for _, tuple := range tuples {
		err := page.AppendTuple(tuple)
		testutil.MustBeNil(t, err)
	}

	header := page.decodeHeader()
	testutil.MustEqual(t, header.id, PageID(42))
	testutil.MustEqual(t, header.tuplesCount, uint16(4))
	testutil.MustEqual(t, len(header.slots), 4)

	for i := 0; i < int(header.tuplesCount); i++ {
		slot := header.slots[i]
		tp := DeserializeTuple(page.bs[slot.offset : slot.offset+slot.length])
		testutil.MustEqual(t, tp, tuples[i])
	}

	// make sure err is responded when the page has no space
	tuple := &Tuple{
		Data: []*TupleData{
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

	page = InitPage(50)
	// append $max tuples in the page.
	// Error should not happen.
	for i := 0; i < max; i++ {
		err := page.AppendTuple(tuple)
		testutil.MustBeNil(t, err)
	}

	// because the page already contains $max tuples,
	// no available space error must happen.
	err := page.AppendTuple(tuple)
	testutil.MustEqual(t, err == nil, false)
}
