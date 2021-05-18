package ssdb

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path"
	"strings"

	"github.com/dty1er/sdb/btree"
)

// DiskManager manages actual file on the disk.
// Storage engine should never touch the disk directory. Instead,
// it should be done through disk manager.
type DiskManager struct {
	directory string
}

func NewDiskManager(directory string) *DiskManager {
	return &DiskManager{directory: directory}
}

func (dm *DiskManager) GetPage(loc *pageLocation) (*Page, error) {
	filename := path.Join(dm.directory, loc.Filename)
	file, err := os.OpenFile(filename, os.O_RDONLY, 0755)
	if err != nil {
		return nil, fmt.Errorf("open file %s: %w", filename, err)
	}
	defer file.Close()

	var buff [PageSize]byte
	if _, err = file.ReadAt(buff[:], int64(loc.Offset)); err != nil {
		return nil, fmt.Errorf("read file %s at %d: %w", filename, loc.Offset, err)
	}

	return &Page{bs: buff}, nil
}

func (dm *DiskManager) PersistPage(loc *pageLocation, page *Page) error {
	filename := path.Join(dm.directory, loc.Filename)
	file, err := os.OpenFile(filename, os.O_RDWR|os.O_CREATE, 0755)
	if err != nil {
		return fmt.Errorf("open file %s: %w", filename, err)
	}
	defer file.Close()

	if _, err = file.WriteAt(page.bs[:], int64(loc.Offset)); err != nil {
		return fmt.Errorf("write page on the file %s at %d: %w", filename, loc.Offset, err)
	}

	return nil
}

func (dm *DiskManager) LoadIndex() (map[string]*btree.BTree, error) {
	files, err := os.ReadDir(dm.directory)
	if err != nil {
		return nil, fmt.Errorf("read dir %s: %w", dm.directory, err)
	}

	indexFiles := []string{}
	for _, file := range files {
		if file.IsDir() {
			continue
		}
		filename := file.Name()
		if strings.HasSuffix(filename, ".idx") {
			indexFiles = append(indexFiles, filename)
		}
	}

	indices := map[string]*btree.BTree{}
	for _, indexFile := range indexFiles {
		file, err := os.OpenFile(indexFile, os.O_RDONLY, 0755)
		if err != nil {
			return nil, fmt.Errorf("open file %s: %w", indexFile, err)
		}

		val, err := io.ReadAll(file)
		if err != nil {
			return nil, fmt.Errorf("read file %s: %w", indexFile, err)
		}

		bt, err := btree.Deserialize(val)
		if err != nil {
			return nil, fmt.Errorf("deserialize json file %s: %w", indexFile, err)
		}

		table := strings.TrimSuffix(indexFile, ".idx")
		indices[table] = bt
	}

	return indices, nil
}

func (dm *DiskManager) PersistIndex(table string, index *btree.BTree) error {
	bs, err := index.Serialize()
	if err != nil {
		return fmt.Errorf("serialize index for %s: %w", table, err)
	}

	filename := path.Join(dm.directory, fmt.Sprintf("%s.idx", table))
	file, err := os.OpenFile(filename, os.O_RDWR|os.O_CREATE, 0755)
	if err != nil {
		return fmt.Errorf("open file %s: %w", filename, err)
	}
	defer file.Close()

	if _, err = file.Write(bs); err != nil {
		return fmt.Errorf("write index on the file %s: %w", filename, err)
	}

	return nil
}

func (dm *DiskManager) LoadPageDirectory() (*PageDirectory, error) {
	filename := path.Join(dm.directory, "__page_directory.db")
	file, err := os.OpenFile(filename, os.O_RDONLY, 0755)
	if err != nil {
		return nil, fmt.Errorf("open file %s, %w", filename, err)
	}

	var pd PageDirectory
	if err := json.NewDecoder(file).Decode(&pd); err != nil {
		return nil, fmt.Errorf("deserialize json file %s, %w", filename, err)
	}

	return &pd, nil
}

func (dm *DiskManager) PersistPageDirectory(pd *PageDirectory) error {
	buff := new(bytes.Buffer)
	if err := json.NewEncoder(buff).Encode(&pd); err != nil {
		return fmt.Errorf("serialize page directory: %w", err)
	}

	filename := path.Join(dm.directory, "__page_directory.db")
	file, err := os.OpenFile(filename, os.O_RDWR|os.O_CREATE, 0755)
	if err != nil {
		return fmt.Errorf("open file %s, %w", filename, err)
	}
	defer file.Close()

	if _, err = file.Write(buff.Bytes()); err != nil {
		return fmt.Errorf("write page directory on the file %s: %w", filename, err)
	}

	return nil
}
