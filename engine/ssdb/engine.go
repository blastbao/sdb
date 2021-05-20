package ssdb

import (
	"fmt"
	"sort"

	"github.com/dty1er/sdb/btree"
	"github.com/dty1er/sdb/config"
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

func New() (*Engine, error) {
	// TODO: this should be passed as arg
	conf, err := config.Process()
	if err != nil {
		return nil, err
	}

	diskManager := NewDiskManager(conf.Server.DBFilesDirectory)

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

	bufferPool := NewBufferPool(conf.Server.BufferPoolEntryCount, indices)

	return &Engine{
		bufferPool:    bufferPool,
		pageDirectory: pageDirectory,
		diskManager:   diskManager,
	}, nil
}

// CreateIndex initializes the btree index.
func (e *Engine) CreateIndex(idxName string) {
	bt := btree.New()

	e.bufferPool.indices[idxName] = bt
}

// InsertIndex inserts a record to the index
func (e *Engine) InsertIndex(idxName string, t *Tuple) error {
	index := e.bufferPool.indices[idxName]
	index.Put(t)

	return nil
}

// InsertTuple inserts a record to the given table.
func (e *Engine) InsertTuple(table string, t *Tuple) error {
	var pageID PageID
	pageIDs := e.pageDirectory.GetPageIDs(table)
	if len(pageIDs) == 0 {
		// First record for the table. Insert a page
		page := InitPage(1)
		e.insertPage(table, page)
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
		if err := e.insertPage(table, page); err != nil {
			return err
		}

		pageID = page.GetID()
	}

	return nil
}

func (e *Engine) ReadIndex(idxName string) *btree.BTree {
	// FUTURE WORK: it assumes every index is cached in buffer pool, but
	// it makes sdb require a lot of memory. Some of them should be cached but
	// some should be on disk.
	return e.bufferPool.readIndex(idxName)
}

func (e *Engine) ReadTable(table string) ([]*Tuple, error) {
	tuples := []*Tuple{}
	pageIDs := e.pageDirectory.GetPageIDs(table)
	for _, pageID := range pageIDs {
		page := e.bufferPool.GetPage(table, pageID)
		if page == nil {
			loc, err := e.pageDirectory.GetPageLocation(table, pageID)
			if err != nil {
				panic(err) // this must not happen
			}
			p, err := e.diskManager.GetPage(loc)
			if err != nil {
				return nil, err
			}
			page = p
		}

		ts := page.GetTuples()
		tuples = append(tuples, ts...)
	}

	// default sort by key
	sort.Slice(tuples, func(i, j int) bool { return tuples[i].Less(tuples[j]) })

	return tuples, nil
}

// insertPage inserts a given page in pageDirectory and buffer pool.
func (e *Engine) insertPage(table string, page *Page) error {
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
