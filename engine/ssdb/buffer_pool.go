package ssdb

import (
	"crypto/sha256"
	"fmt"

	"github.com/dty1er/sdb/lru"
)

type BufferPool struct {
	frames        *lru.Cache
	pageDirectory *PageDirectory
	// TODO: keep b-tree index here
}

func NewBufferPool(entryCount int) (*BufferPool, error) {
	frames := lru.New(lru.WithCap(entryCount))
	pageDirectory, err := LoadPageDirectory()
	if err != nil {
		return nil, err
	}

	return &BufferPool{
		frames:        frames,
		pageDirectory: pageDirectory,
	}, nil
}
