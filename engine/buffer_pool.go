package engine

import (
	"crypto/sha256"
	"fmt"

	"github.com/dty1er/sdb/btree"
	"github.com/dty1er/sdb/lru"
	"github.com/dty1er/sdb/sdb"
)

// pageDescriptor is a management unit of page from buffer pool point of view.
type pageDescriptor struct {
	table string
	page  *Page

	// when dirty flag is true, the page on the buffer pool
	dirty bool
}

type IndexKey string

func toIndexKey(table, idxName string) IndexKey {
	return IndexKey(fmt.Sprintf("%s__%s", table, idxName))
}

// BufferPool manages pages, indices, and files on the disk.
type BufferPool struct {
	// lru cache element type is *PageDescriptor. BufferPool never manages Page directly.
	frames  *lru.Cache
	indices map[IndexKey]*btree.BTree
}

func NewBufferPool(entryCount int, indices map[IndexKey]*btree.BTree) *BufferPool {
	frames := lru.New(lru.WithCap(entryCount))
	return &BufferPool{frames: frames, indices: indices}
}

// cacheKey encodes the cache key from the given arguments.
//
//
func (bp *BufferPool) cacheKey(tableName string, pageID PageID) string {
	hash := sha256.Sum256([]byte(fmt.Sprintf("%s___%d", tableName, pageID)))
	return string(hash[:])
}

func (bp *BufferPool) readIndex(table, idxName string) *btree.BTree {
	key := toIndexKey(table, idxName)
	return bp.indices[key]
}

// FindPage returns if the buffer pool has the page.
func (bp *BufferPool) FindPage(tableName string, pageID PageID) bool {
	key := bp.cacheKey(tableName, pageID)
	return bp.frames.Get(key) != nil
}

func (bp *BufferPool) GetPage(tableName string, pageID PageID) *Page {
	// table:page
	key := bp.cacheKey(tableName, pageID)
	elem := bp.frames.Get(key)
	if elem == nil {
		return nil
	}
	return elem.(*pageDescriptor).page
}

// InsertPage inserts page in the cache.
// When non-nil page is returned, it must be persisted on the disk.
func (bp *BufferPool) InsertPage(tableName string, page *Page) *Page {
	// 缓存键
	key := bp.cacheKey(tableName, page.GetID())

	// when inserting a new page, it is not persisted so dirty must be true
	// 新插入的页一定是脏页
	pd := &pageDescriptor{
		table: tableName,
		page: page,
		dirty: true,
	}

	// 插入新页，返回被淘汰页
	evicted := bp.frames.Set(key, pd)
	if evicted == nil {
		return nil
	}

	// 如果被淘汰的是脏页，需要返回，调用者会执行刷盘，否则返回 nil
	evictedPageDescriptor := evicted.(*pageDescriptor)
	if !evictedPageDescriptor.dirty {
		return nil
	}

	return evictedPageDescriptor.page

}

// AppendTuple finds the page from page directory then puts tuple in it.
// If the page is not found, false will be responded.
func (bp *BufferPool) AppendTuple(tableName string, pageID PageID, tuple sdb.Tuple) bool {
	key := bp.cacheKey(tableName, pageID)

	// First, try to fetch the page for the table from cache
	// 查询页
	elem := bp.frames.Get(key)
	if elem == nil {
		return false // when page is not found in the cache, return false
	}

	pageDescriptor := elem.(*pageDescriptor)
	// When cache is found, try to append the tuple to it
	// When the page doesn't have enough space, it returns false
	if err := pageDescriptor.page.AppendTuple(tuple); err != nil {
		// no available space. new page should be created in advance
		return false
	}

	pageDescriptor.dirty = true // when new tuple is appended to the page, it is marked dirty
	return true
}
