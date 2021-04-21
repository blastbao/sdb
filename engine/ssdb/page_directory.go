package ssdb

import (
	"fmt"
	"sort"
	"strconv"
	"strings"
)

func EncodePageDirectoryID(table string, pageID PageID) string {
	return fmt.Sprintf("%s#%d", table, pageID)
}

type pageLocation struct {
	filename string
	offset   uint32
	// length is always PageSize
}

// PageDirectory manages page location by table name and page id.
// This information is persisted on the disk.
type PageDirectory struct {
	pageIDs      map[string][]PageID // table name and PageID
	pageLocation map[PageDirectoryID]pageLocation
}

// LoadPageDirectory loads page directory from the file on the disk.
func LoadPageDirectory() (*PageDirectory, error) {
	// TODO: decide how to encode/decode page directory file
	return nil, nil
}

// Save saves page directory on the file on the disk.
func (pd *PageDirectory) Save() error {
	// TODO: decide how to encode/decode page directory file
	return nil
}

func (pd *PageDirectory) GetPageIDs(table string) []PageID {
	return pd.pageIDs[table]
}

// GetPageContent gets page by given table name and pageID.
func (pd *PageDirectory) GetPageContent(table string, pageID PageID) (*Page, error) {
	pdid := EncodePageDirectoryID(table, pageID)
	loc, ok := pd.pageLocation[pdid]
	if !ok {
		return nil, fmt.Errorf("page not found")
	}

	f, err := os.OpenFile(loc.filename, os.O_RDONLY, 0755)
	if err != nil {
		return nil, fmt.Errorf("open page file: %w", err)
	}

	buff := make([]byte, PageSize)
	_, err = f.ReadAt(buff, int64(loc.offset))
	if err != nil {
		return nil, fmt.Errorf("read page file: %w", err)
	}

	var bs [PageSize]byte
	copy(bs[:], buff)

	return &Page{bs: bs}, nil
}
