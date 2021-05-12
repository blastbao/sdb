package ssdb

import (
	"fmt"
	"os"
	"path"
)

// DiskManager manages actual file on the disk.
type DiskManager struct {
	directory string
}

func NewDiskManager(directory string) *DiskManager {
	return &DiskManager{directory: directory}
}

func (dm *DiskManager) GetPage(loc *pageLocation) (*Page, error) {
	filename := path.Join(dm.directory, loc.filename)
	file, err := os.OpenFile(filename, os.O_RDONLY, 0755)
	if err != nil {
		return nil, fmt.Errorf("open file %s, %w", filename, err)
	}

	var buff [PageSize]byte
	if _, err = file.ReadAt(buff[:], int64(loc.offset)); err != nil {
		return nil, fmt.Errorf("read file %s at %d, %w", filename, loc.offset, err)
	}

	return &Page{bs: buff}, nil
}

func (dm *DiskManager) PersistPage(loc *pageLocation, page *Page) error {
	filename := path.Join(dm.directory, loc.filename)
	file, err := os.OpenFile(filename, os.O_RDWR|os.O_CREATE, 0755)
	if err != nil {
		return fmt.Errorf("open file %s, %w", filename, err)
	}

	if _, err = file.WriteAt(page.bs[:], int64(loc.offset)); err != nil {
		return fmt.Errorf("write page on the file %s at %d, %w", filename, loc.offset, err)
	}

	return nil
}
