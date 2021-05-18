package ssdb

import (
	"bytes"
	"encoding/json"
	"fmt"
	"os"
	"path"
	"sort"
	"strconv"
	"strings"
)

const (
	MaxPageCountPerFile = 1000
)

func EncodePageDirectoryID(table string, pageID PageID) string {
	return fmt.Sprintf("%s#%d", table, pageID)
}

type pageLocation struct {
	Filename string
	Offset   uint32
	// length is always PageSize
}

// PageDirectory manages page location by table name and page id.
// This information is persisted on the disk.
type PageDirectory struct {
	PageIDs             map[string][]PageID // table name and PageID
	PageLocation        map[string]*pageLocation
	MaxPageCountPerFile int
}

// LoadPageDirectory loads page directory from the file on the disk.
func LoadPageDirectory(directory string) (*PageDirectory, error) {
	var pd PageDirectory
	filename := path.Join(directory, "__page_directory.db")

	f, err := os.OpenFile(filename, os.O_RDONLY|os.O_CREATE, 0755)
	if err != nil {
		return nil, fmt.Errorf("open file %s: %w", filename, err)
	}

	info, err := f.Stat()
	if err != nil {
		return nil, fmt.Errorf("stat file %s: %w", filename, err)
	}

	if info.Size() == 0 {
		// no page exists (e.g. run database for the first time)
		return &PageDirectory{
			PageIDs:             map[string][]PageID{},
			PageLocation:        map[string]*pageLocation{},
			MaxPageCountPerFile: MaxPageCountPerFile,
		}, nil
	}

	if err := json.NewDecoder(f).Decode(&pd); err != nil {
		return nil, fmt.Errorf("decode file %s: %w", filename, err)
	}

	return &pd, nil
}

// Save saves page directory on the file on the disk.
func (pd *PageDirectory) Save(directory string) error {
	filename := path.Join(directory, "__page_directory.db")
	buff := new(bytes.Buffer)

	if err := json.NewEncoder(buff).Encode(pd); err != nil {
		return err
	}

	f, err := os.OpenFile(filename, os.O_RDWR|os.O_CREATE, 0755)
	if err != nil {
		return fmt.Errorf("open file %s: %w", filename, err)
	}

	if _, err := f.Write(buff.Bytes()); err != nil {
		return fmt.Errorf("write file %s: %w", filename, err)
	}

	return nil
}

func (pd *PageDirectory) GetPageIDs(table string) []PageID {
	return pd.PageIDs[table]
}

func (pd *PageDirectory) RegisterPage(table string, page *Page) {
	pdid := EncodePageDirectoryID(table, page.GetID())
	ids := pd.PageIDs[table]
	if len(ids) == 0 {
		pd.PageIDs[table] = []PageID{page.GetID()}
		pd.PageLocation[pdid] = &pageLocation{Filename: toFilename(table, 1), Offset: 0}
		return
	}

	pd.PageIDs[table] = append(ids, page.GetID())

	filenames := []string{}
	pageCounts := map[string]int{}
	for _, pageID := range ids {
		loc := pd.PageLocation[EncodePageDirectoryID(table, pageID)]
		filenames = append(filenames, loc.Filename)
		if _, ok := pageCounts[loc.Filename]; ok {
			pageCounts[loc.Filename]++
		} else {
			pageCounts[loc.Filename] = 1
		}
	}

	sort.Strings(filenames)
	latestFilename := filenames[len(filenames)-1]
	pageCount := pageCounts[latestFilename]

	// when the latest file has enough space to store a page,
	// use the file
	if pageCount < pd.MaxPageCountPerFile {
		newPageLoc := &pageLocation{Filename: latestFilename, Offset: uint32(pageCount * PageSize)}
		pd.PageLocation[pdid] = newPageLoc
		return
	}

	// define new file
	_, offset := fileInfofromFilename(latestFilename)
	newPageLoc := &pageLocation{Filename: toFilename(table, offset+1), Offset: 0}
	pd.PageLocation[pdid] = newPageLoc
}

// GetPageLocation gets page by given table name and pageID.
func (pd *PageDirectory) GetPageLocation(table string, pageID PageID) (*pageLocation, error) {
	pdid := EncodePageDirectoryID(table, pageID)
	loc, ok := pd.PageLocation[pdid]
	if !ok {
		return nil, fmt.Errorf("page not found for table %v, id %v", table, pageID)
	}

	return loc, nil
}

func toFilename(table string, offset int) string {
	return fmt.Sprintf("%s__%d.db", table, offset)
}

func fileInfofromFilename(filename string) (string, int) {
	splitted := strings.Split(filename, "__")
	// assuming page file name must be table_name__offset.db, unless panic.
	table := splitted[0]
	offsetS := strings.TrimSuffix(splitted[1], ".db")
	offset, _ := strconv.Atoi(offsetS)
	return table, offset
}
