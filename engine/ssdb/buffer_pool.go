package ssdb

import "github.com/dty1er/sdb/lru"

type BufferPool struct {
	frames *lru.Cache
	// pageTable map[pageID]string
	// TODO: keep b-tree index here
}

// PutTuple puts tuple in the buffer pool.
//
func (bp *BufferPool) PutTuple(tableName string, tuple *Tuple) {

}
