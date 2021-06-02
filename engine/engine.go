package engine

import (
	"fmt"
	"sort"

	"github.com/dty1er/sdb/btree"
	"github.com/dty1er/sdb/config"
	"github.com/dty1er/sdb/sdb"
)

func init() {
	btree.RegisterSerializationTarget(&Tuple{})
}

// Engine is sdb core storage engine.
// It implements sdb/engine.Engine interface.
type Engine struct {
	bufferPool    *BufferPool
	pageDirectory *PageDirectory

	catalog     sdb.Catalog
	diskManager sdb.DiskManager
}

func New(conf *config.Server, catalog sdb.Catalog, diskManager sdb.DiskManager) (*Engine, error) {
	indices := make(map[IndexKey]*btree.BTree)
	indexCatalog := catalog.ListIndices()
	for _, index := range indexCatalog {
		bt := btree.New()
		if err := diskManager.Load(string(toIndexKey(index.Table, index.Name))+".idx", 0, bt); err != nil {
			return nil, err
		}

		key := toIndexKey(index.Table, string(index.Name))
		indices[key] = bt
	}
	if len(indices) == 0 {
		indices = map[IndexKey]*btree.BTree{}
	}

	pageDirectory := &PageDirectory{}
	if err := diskManager.Load("__page_directory.db", 0, pageDirectory); err != nil {
		return nil, err
	}

	bufferPool := NewBufferPool(conf.BufferPoolEntryCount, indices)

	return &Engine{
		bufferPool:    bufferPool,
		pageDirectory: pageDirectory,
		catalog:       catalog,
		diskManager:   diskManager,
	}, nil
}

// CreateIndex initializes the btree index.
func (e *Engine) CreateIndex(table, idxName string) {
	bt := btree.New()

	key := toIndexKey(table, idxName)
	e.bufferPool.indices[key] = bt
}

// InsertIndex inserts a record to the index
func (e *Engine) InsertIndex(table, idxName string, t *Tuple) error {
	key := toIndexKey(table, idxName)
	index := e.bufferPool.indices[key]
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
			var p Page
			if err := e.diskManager.Load(loc.Filename, int(loc.Offset), &p); err != nil {
				return err
			}

			evicted := e.bufferPool.InsertPage(table, &p)
			if evicted != nil {
				loc, err := e.pageDirectory.GetPageLocation(table, evicted.GetID())
				if err != nil {
					return err
				}
				if err = e.diskManager.Persist(loc.Filename, int(loc.Offset), evicted); err != nil {
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

func (e *Engine) ReadIndex(table, idxName string) *btree.BTree {
	// FUTURE WORK: it assumes every index is cached in buffer pool, but
	// it makes sdb require a lot of memory. Some of them should be cached but
	// some should be on disk.
	return e.bufferPool.readIndex(table, idxName)
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
			var p Page
			if err := e.diskManager.Load(loc.Filename, int(loc.Offset), &p); err != nil {
				return nil, err
			}
			page = &p
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
		if err = e.diskManager.Persist(loc.Filename, int(loc.Offset), evicted); err != nil {
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
			if err := e.diskManager.Persist(loc.Filename, int(loc.Offset), pd.page); err != nil {
				return err
			}
		}
	}

	// persist indices
	for idxKey, index := range e.bufferPool.indices {
		if err := e.diskManager.Persist(string(idxKey)+".idx", 0, index); err != nil {
			return err
		}
	}

	// persist pageDirectory
	if err := e.diskManager.Persist("__page_directory.db", 0, e.pageDirectory); err != nil {
		return err
	}

	return nil
}
