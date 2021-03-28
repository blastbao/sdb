package ssdb

import (
	"crypto/sha256"
	"fmt"

	"github.com/dty1er/sdb/lru"
)

type BufferPool struct {
	frames        *lru.Cache
	pageDirectory PageDirectory
	// TODO: keep b-tree index here
}

// PutTuple puts tuple in the buffer pool.
//
func (bp *BufferPool) PutTuple(tableName string, tuple *Tuple) {

}
