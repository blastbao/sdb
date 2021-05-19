package ssdb

import (
	"fmt"

	"github.com/dty1er/sdb/btree"
)

func init() {
	btree.RegisterSerializationTarget(&Tuple{})
}

// Engine is ssdb core storage engine.
// It implements sdb/engine.Engine interface.
type Engine struct {
	bufferPool    *BufferPool
	pageDirectory *PageDirectory
	diskManager   *DiskManager
}

// config is the configuration of ssdb storage engine.
type config struct {
	BufferPoolEntryCount int
	DBFilesDirectory     string
}

func New() (*Engine, error) {
	// TODO: load config file
	config := &config{
		BufferPoolEntryCount: 1000,
		DBFilesDirectory:     "./db/",
	}

	diskManager := NewDiskManager(config.DBFilesDirectory)

	indices, err := diskManager.LoadIndex()
	if err != nil {
		return nil, err
	}
	if len(indices) == 0 {
		indices = map[string]*btree.BTree{}
	}

	pageDirectory, err := diskManager.LoadPageDirectory()
	if err != nil {
		return nil, err
	}

	bufferPool := NewBufferPool(config.BufferPoolEntryCount, indices)

	return &Engine{
		bufferPool:    bufferPool,
		pageDirectory: pageDirectory,
		diskManager:   diskManager,
	}, nil
}

type KeyType uint8

const (
	Int KeyType = iota + 1
	String
)

// CreateIndex initializes the btree index.
func (e *Engine) CreateIndex(idxName string, keyType KeyType) {
	var bt *btree.BTree
	if keyType == Int {
		bt = btree.NewIntKeyTree()
	} else {
		bt = btree.NewStringKeyTree()
	}

	e.bufferPool.indices[idxName] = bt
}

// InsertIndex inserts a record to the index
func (e *Engine) InsertIndex(idxName string, key int, t *Tuple) error {
	index := e.bufferPool.indices[idxName]
	index.Put(key, t)

	return nil
}

// InsertTuple inserts a record to the given table.
func (e *Engine) InsertTuple(table string, t *Tuple) error {
	var pageID PageID
	pageIDs := e.pageDirectory.GetPageIDs(table)
	if len(pageIDs) == 0 {
		// First record for the table. Insert a page
		page := InitPage(1)
		e.InsertPage(table, page)
		pageID = PageID(1)
	} else {
		// use the last page
		pageID = pageIDs[len(pageIDs)-1]
	}

	for {
		// first, make sure the page is on the buffer pool
		pageFound := e.bufferPool.FindPage(table, pageID)
		if !pageFound {
			// if not found, put the page on the cache
			loc, err := e.pageDirectory.GetPageLocation(table, pageID)
			if err != nil {
				// this must not happen
				panic(fmt.Sprintf("page is not found in the page directory: %s", err))
			}
			p, err := e.diskManager.GetPage(loc)
			if err != nil {
				return err
			}

			evicted := e.bufferPool.InsertPage(table, p)
			if evicted != nil {
				loc, err := e.pageDirectory.GetPageLocation(table, evicted.GetID())
				if err != nil {
					return err
				}
				if err = e.diskManager.PersistPage(loc, evicted); err != nil {
					return err
				}
			}
		}

		// try to append the tuple on the page
		appended := e.bufferPool.AppendTuple(table, pageID, t)
		if appended {
			// if append succeeds, finish
			break
		}

		// if fail, init new page then try to use it
		page := InitPage(uint32(pageID) + 1)
		if err := e.InsertPage(table, page); err != nil {
			return err
		}

		pageID = page.GetID()
	}

	return nil
}

// InsertPage inserts a given page in pageDirectory and buffer pool.
func (e *Engine) InsertPage(table string, page *Page) error {
	e.pageDirectory.RegisterPage(table, page)
	evicted := e.bufferPool.InsertPage(table, page)
	if evicted != nil {
		loc, err := e.pageDirectory.GetPageLocation(table, evicted.GetID())
		if err != nil {
			return err
		}
		if err = e.diskManager.PersistPage(loc, evicted); err != nil {
			return err
		}
	}

	return nil
}

// Shutdown shuts down the ssdb storage engine.
// When the database stops, this method must be called.
func (e *Engine) Shutdown() error {
	// persist pages
	elements := e.bufferPool.frames.GetAll()
	for _, elem := range elements {
		pd := elem.(*pageDescriptor)
		if pd.dirty {
			loc, err := e.pageDirectory.GetPageLocation(pd.table, pd.page.GetID())
			if err != nil {
				return err
			}
			if err := e.diskManager.PersistPage(loc, pd.page); err != nil {
				return err
			}
		}
	}

	// persist indices
	for table, index := range e.bufferPool.indices {
		if err := e.diskManager.PersistIndex(table, index); err != nil {
			return err
		}
	}

	// persist pageDirectory
	if err := e.diskManager.PersistPageDirectory(e.pageDirectory); err != nil {
		return err
	}

	return nil
}
