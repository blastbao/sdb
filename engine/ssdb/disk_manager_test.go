package ssdb

import (
	"os"
	"path"
	"testing"

	"github.com/dty1er/sdb/testutil"
)

func TestDiskManager_GetPage(t *testing.T) {
	tempDir := os.TempDir()
	// testdata
	tuple := Tuple{Data: []TupleData{
		{Typ: Int32, Int32Val: 96},
		{Typ: Byte64, Byte64Val: [64]byte{'a', 'b', 'c'}},
	}}
	page := NewPage(42)
	page.AppendTuple(tuple)

	filename := "users__1.db"
	offset := 0

	file, err := os.OpenFile(path.Join(tempDir, filename), os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0755)
	if err != nil {
		t.Fatalf("failed to open file %s: %s", filename, err)
	}
	defer os.Remove(path.Join(tempDir, filename))

	file.WriteAt(page.bs[:], int64(offset))

	loc := &pageLocation{Filename: filename, Offset: uint32(offset)}

	dm := NewDiskManager(tempDir)
	p, err := dm.GetPage(loc)
	if err != nil {
		t.Fatalf("unexpected error: %s", err)
	}

	testutil.MustEqual(t, p, page)
}

func TestDiskManager_PersistPage(t *testing.T) {
	filename := "users__2.db"
	offset := 0
	loc := &pageLocation{Filename: filename, Offset: uint32(offset)}

	tuple := Tuple{Data: []TupleData{
		{Typ: Int32, Int32Val: 96},
		{Typ: Byte64, Byte64Val: [64]byte{'a', 'b', 'c'}},
	}}
	page := NewPage(42)
	page.AppendTuple(tuple)

	tempDir := os.TempDir()
	dm := NewDiskManager(tempDir)
	if err := dm.PersistPage(loc, page); err != nil {
		t.Fatalf("unexpected error: %s", err)
	}

	file, err := os.OpenFile(path.Join(tempDir, filename), os.O_RDONLY, 0755)
	if err != nil {
		t.Fatalf("failed to open file %s: %s", filename, err)
	}
	defer os.Remove(path.Join(tempDir, filename))

	var bs [PageSize]byte
	n, err := file.ReadAt(bs[:], int64(offset))
	if err != nil {
		t.Fatalf("failed to read file %s: %s", filename, err)
	}

	testutil.MustEqual(t, n, PageSize)
	testutil.MustEqual(t, bs, page.bs)
}
