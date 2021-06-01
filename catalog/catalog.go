package catalog

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"sync"

	"github.com/dty1er/sdb/schema"
	"github.com/dty1er/sdb/sdb"
)

// Catalog is a set of information/metadata about the sdb instance. It never contain actual data.
// Catalog must implement sdb.Catalog, Serializer and Deserializer interfaces.
type Catalog struct {
	Tables map[string]*schema.Table
	// FUTURE WORK: Add views, users, etc.
	latch sync.RWMutex `json:"-"`

	diskManager sdb.DiskManager
}

func NewEmptyCatalog() *Catalog {
	return &Catalog{
		Tables: map[string]*schema.Table{},
	}
}

func New(dm sdb.DiskManager) (*Catalog, error) {
	var c Catalog
	if err := dm.Load("__catalog.db", 0, &c); err != nil {
		return nil, err
	}

	if len(c.Tables) == 0 {
		return NewEmptyCatalog(), nil
	}

	c.diskManager = dm

	return &c, nil
}

func (c *Catalog) AddTable(table string, columns []*schema.ColumnDef, indices []*schema.Index) error {
	c.latch.Lock()
	defer c.latch.Unlock()

	if c.FindTable(table) {
		return fmt.Errorf("table %s already exists", table)
	}

	c.Tables[table] = &schema.Table{Columns: columns, Indices: indices}

	return nil
}

func (c *Catalog) FindTable(table string) bool {
	_, ok := c.Tables[table]
	return ok
}

func (c *Catalog) ListIndices() []*schema.Index {
	indices := []*schema.Index{}
	for _, table := range c.Tables {
		indices = append(indices, table.Indices...)
	}
	return indices
}

func (c *Catalog) Persist() error {
	if err := c.diskManager.Persist("__catalog.db", 0, c); err != nil {
		return err
	}

	return nil
}

func (c *Catalog) Serialize() ([]byte, error) {
	var buff bytes.Buffer
	if err := json.NewEncoder(&buff).Encode(c); err != nil {
		return nil, err
	}

	return buff.Bytes(), nil
}

func (c *Catalog) Deserialize(r io.Reader) error {
	if err := json.NewDecoder(r).Decode(c); err != nil {
		return err
	}

	return nil
}
