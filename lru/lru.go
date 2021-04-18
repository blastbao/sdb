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

type Config struct {
	capacity int
}

type Option func(c *Config)

func New(opts ...Option) *Cache {
	conf := &Config{}

	for _, opt := range opts {
		opt(conf)
	}

	c := &Cache{
		capacity: conf.capacity,
		items:    make(map[string]*element),
		list:     newDoublyLinkedList(),
		m:        &sync.RWMutex{},
	}

	if c.capacity == 0 {
		c.capacity = 1000
	}

	return c
}

func WithCap(capacity int) Option {
	return func(c *Config) { c.capacity = capacity }
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
// It returns evicted element when eviction arises.
func (c *Cache) Set(key string, value interface{}) interface{} {
	c.m.Lock()
	defer c.m.Unlock()
	_, ok := c.items[key]
	if ok {
		c.items[key].value = value
		c.list.moveToHead(c.items[key])
		return nil
	}

	e := newElement(key, value)
	c.items[key] = e
	c.list.add(e)

	// eviction
	if len(c.items) > c.capacity {
		evictionTargetValue := c.list.tail.prev.value
		delete(c.items, c.list.tail.prev.key) // list.tail is dummy, list.tail.prev is true tail
		c.list.remove(c.list.tail.prev)
		return evictionTargetValue
	}

	return nil
}
