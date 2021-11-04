package diskmanager

import (
	"fmt"
	"os"
	"path"

	"github.com/dty1er/sdb/sdb"
)

type DiskManager struct {
	directory string
}

func New(directory string) *DiskManager {
	return &DiskManager{
		directory: directory,
	}
}

func (dm *DiskManager) Load(name string, offset int, d sdb.Deserializer) error {
	filename := path.Join(dm.directory, name)
	if _, err := os.Stat(filename); err != nil {
		return nil
	}

	file, err := os.OpenFile(filename, os.O_RDONLY, 0755)
	if err != nil {
		return fmt.Errorf("open file %s: %w", filename, err)
	}
	defer file.Close()

	if err := d.Deserialize(file); err != nil {
		return fmt.Errorf("deserialize file %s: %w", filename, err)
	}

	return nil
}

func (dm *DiskManager) Persist(name string, offset int, page sdb.Serializer) error {
	// 打开数据文件
	filename := path.Join(dm.directory, name)
	file, err := os.OpenFile(filename, os.O_RDWR|os.O_CREATE, 0755)
	if err != nil {
		return fmt.Errorf("open file %s: %w", filename, err)
	}
	defer file.Close()

	// 页序列化
	serialized, err := page.Serialize()
	if err != nil {
		return fmt.Errorf("serialize %s: %w", filename, err)
	}

	// 写入页
	if _, err = file.WriteAt(serialized, int64(offset)); err != nil {
		return fmt.Errorf("write page on the file %s at %d: %w", filename, offset, err)
	}

	return nil
}
