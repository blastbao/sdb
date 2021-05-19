package lru

import (
	"sort"
	"testing"

	"github.com/dty1er/sdb/testutil"
)

func Test_Cache(t *testing.T) {
	c1 := New()
	testutil.MustEqual(t, c1.capacity, 1000)

	c2 := New(WithCap(5))
	testutil.MustEqual(t, c2.capacity, 5)

	// helper func
	keysSorted := func(m map[string]*element) []string {
		ks := make([]string, 0, len(m))
		for k := range m {
			ks = append(ks, k)
		}
		sort.Strings(ks)
		return ks
	}

	// first, the cache contains 1-5 elements
	// evicted should be nil
	evicted := c2.Set("1", 1)
	testutil.MustEqual(t, evicted == nil, true)

	evicted = c2.Set("2", 2)
	testutil.MustEqual(t, evicted == nil, true)

	evicted = c2.Set("3", 3)
	testutil.MustEqual(t, evicted == nil, true)

	evicted = c2.Set("4", 4)
	testutil.MustEqual(t, evicted == nil, true)

	evicted = c2.Set("5", 5)
	testutil.MustEqual(t, evicted == nil, true)

	testutil.MustEqual(t, len(c2.items), 5)

	// set 6, then first "1" will be evicted
	evicted = c2.Set("6", 6)
	testutil.MustEqual(t, evicted.(int), 1)
	testutil.MustEqual(t, len(c2.items), 5)
	testutil.MustEqual(t, keysSorted(c2.items), []string{"2", "3", "4", "5", "6"})

	// get "2", then it will be marked as it is recently used
	got := c2.Get("2")
	testutil.MustEqual(t, got.(int), 2)

	// then set "7", because "2" is recently used, "3" will be evicted
	evicted = c2.Set("7", 7)
	testutil.MustEqual(t, evicted.(int), 3)
	testutil.MustEqual(t, keysSorted(c2.items), []string{"2", "4", "5", "6", "7"})

	gotAllItems := []int{}
	items := c2.GetAll()
	for _, item := range items {
		intItem, ok := item.(int)
		testutil.MustEqual(t, ok, true)

		gotAllItems = append(gotAllItems, intItem)
	}

	sort.Ints(gotAllItems)
	testutil.MustEqual(t, gotAllItems, []int{2, 4, 5, 6, 7})
}
