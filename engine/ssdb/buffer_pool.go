package ssdb

import "github.com/dty1er/sdb/lru"

type bufferPool struct {
	cache *lru.Cache
}
