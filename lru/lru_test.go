package lru

import (
	"reflect"
	"sort"
	"testing"
)

func Test_Cache(t *testing.T) {
	c1 := New()
	if c1.capacity != 1000 {
		t.Errorf("default capacity is not 1000: %d", c1.capacity)
	}

	c2 := New(WithCap(5))
	if c2.capacity != 5 {
		t.Errorf("customized capacity is not 5: %d", c2.capacity)
	}

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
	c2.Set("1", 1)
	c2.Set("2", 2)
	c2.Set("3", 3)
	c2.Set("4", 4)
	c2.Set("5", 5)
	if len(c2.items) != 5 {
		t.Errorf("invalid len: %d", len(c2.items))
	}

	// set 6, then first "1" will be evicted
	c2.Set("6", 6)
	if len(c2.items) != 5 {
		t.Errorf("invalid len: %d", len(c2.items))
	}
	if !reflect.DeepEqual(keysSorted(c2.items), []string{"2", "3", "4", "5", "6"}) {
		t.Errorf("invalid cache: %v", c2.items)
	}

	// get "2", then it will be marked as it is recently used
	_ = c2.Get("2")
	// then set "7", because "2" is recently used, "3" will be evicted
	c2.Set("7", 7)
	if !reflect.DeepEqual(keysSorted(c2.items), []string{"2", "4", "5", "6", "7"}) { // "3" is evicted
		t.Errorf("invalid cache: %v", c2.items)
	}
}
