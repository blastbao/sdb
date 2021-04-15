package ssdb

import (
	"fmt"
	"os"
)

// PageDirectoryID is the id of page directory.
// It consists of table name and page id.
// The ID is mapped to the filename and the location of actual data on the disk.
type PageDirectoryID string

func EncodePageDirectoryID(table string, pageID PageID) PageDirectoryID {
	return PageDirectoryID(fmt.Sprintf("%s#%d", table, pageID))
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
