package lru

import "sync"

type Cache struct {
	capacity int
	// list manages the element age.
	// Head element is the most-recently-used one.
	// On eviction, the tail elem is removed.
	list *doublyLinkedList

	// items is a hash map to the elements.
	items map[string]*element
	m     *sync.RWMutex
}

func New() *Cache {
	return &Cache{
		capacity: 1000,
		items:    make(map[string]*element),
		list:     newDoublyLinkedList(),
	}
}

// Get returns value by given key.
// nil is returned when the key is not found in the cache.
func (c *Cache) Get(key string) interface{} {
	c.m.RLock()
	defer c.m.RUnlock()
	_, ok := c.items[key]
	if !ok {
		return nil
	}

	c.list.moveToHead(c.items[key])
	return c.items[key]

}

// Set sets the value by the given key.
func (c *Cache) Set(key string, value interface{}) {
	c.m.Lock()
	defer c.m.Unlock()
	_, ok := c.items[key]
	if ok {
		c.items[key].value = value
		c.list.moveToHead(c.items[key])
		return
	}

	e := newElement(key, value)
	c.items[key] = e
	c.list.add(e)

	// eviction
	if len(c.items) > c.capacity {
		delete(c.items, c.list.tail.prev.key) // list.tail is dummy, list.tail.prev is true tail
		c.list.remove(c.list.tail.prev)
	}
}
