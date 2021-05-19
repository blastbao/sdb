package ssdb

import (
	"testing"

	"github.com/dty1er/sdb/testutil"
)

func TestBufferPool_InsertPage(t *testing.T) {
	table := "users"
	bp := NewBufferPool(2, nil)

	page1 := InitPage(1)
	page2 := InitPage(2)
	page3 := InitPage(3)

	nilPage := (*Page)(nil)
	evicted := bp.InsertPage(table, page1)
	testutil.MustEqual(t, evicted, nilPage)
	testutil.MustEqual(t, bp.frames.Get(bp.cacheKey(table, 1)).(*pageDescriptor), &pageDescriptor{table: table, page: page1, dirty: true})

	evicted = bp.InsertPage(table, page2)
	testutil.MustEqual(t, evicted, nilPage)
	testutil.MustEqual(t, bp.frames.Get(bp.cacheKey(table, 2)).(*pageDescriptor), &pageDescriptor{table: table, page: page2, dirty: true})

	// because lru capacity is 2, 1st page is evicted when the 3rd page is inserted
	evicted = bp.InsertPage(table, page3)
	testutil.MustEqual(t, evicted, page1)
	testutil.MustEqual(t, bp.frames.Get(bp.cacheKey(table, 3)).(*pageDescriptor), &pageDescriptor{table: table, page: page3, dirty: true})
	testutil.MustEqual(t, bp.frames.Get(bp.cacheKey(table, 1)), nil) // make sure 1st page is evicted

	bp = NewBufferPool(2, nil)

	// set 2 page descriptors whose dirty are false
	bp.frames.Set(bp.cacheKey(table, 1), &pageDescriptor{table: table, page: page1, dirty: false})
	bp.frames.Set(bp.cacheKey(table, 2), &pageDescriptor{table: table, page: page2, dirty: false})

	// page1 is purged but because it's not dirty, it won't be returned.
	evicted = bp.InsertPage(table, page3)
	testutil.MustEqual(t, evicted, nilPage)
}

func TestBufferPool_AppendTuple(t *testing.T) {
	table := "users"
	bp := NewBufferPool(2, nil)

	// make sure false is responded when no page on cache
	appended := bp.AppendTuple(table, 1, &Tuple{})
	testutil.MustEqual(t, appended, false)

	// make sure true is responded when the page is found on cache
	dummyTuple := &Tuple{Data: []*TupleData{{Typ: Int32, Int32Val: 96}}}

	page1 := InitPage(1)
	bp.frames.Set(bp.cacheKey(table, 1), &pageDescriptor{table: table, page: page1, dirty: false})
	appended = bp.AppendTuple(table, 1, &Tuple{})
	testutil.MustEqual(t, appended, true)
	page1Descriptor := bp.frames.Get(bp.cacheKey(table, 1)).(*pageDescriptor)
	testutil.MustEqual(t, page1Descriptor.dirty, true) // make sure the descriptor is marked dirty

	// make sure false is responded when the found page has no enough space

	// append tuples to the page 1 until it has no space
	for {
		if err := page1.AppendTuple(dummyTuple); err != nil {
			break
		}
	}

	appended = bp.AppendTuple(table, 1, dummyTuple)
	testutil.MustEqual(t, appended, false)
}
