package ssdb

import (
	"encoding/json"
	"io"
	"os"
	"path"
	"sort"
	"testing"

	"github.com/dty1er/sdb/btree"
	"github.com/dty1er/sdb/testutil"
)

func TestDiskManager_GetPage(t *testing.T) {
	tempDir := os.TempDir()
	// testdata
	tuple := &Tuple{Data: []*TupleData{
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

	tuple := &Tuple{Data: []*TupleData{
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

func TestDiskManager_LoadIndex(t *testing.T) {
	tempDir := os.TempDir()
	writeIndex := func(t *testing.T, filename string, bt *btree.BTree) {
		file, err := os.OpenFile(path.Join(tempDir, filename), os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0755)
		testutil.MustBeNil(t, err)
		s, err := bt.Serialize()
		testutil.MustBeNil(t, err)
		file.Write(s)
	}
	// testdata 1
	bt := btree.NewIntKeyTree()
	bt.Put(1, "1")
	filename := "user_accounts.idx"
	writeIndex(t, filename, bt)
	defer os.Remove(path.Join(tempDir, filename))

	// testdata 2
	bt2 := btree.NewIntKeyTree()
	bt2.Put(2, "2")
	filename2 := "items.idx"
	writeIndex(t, filename2, bt2)
	defer os.Remove(path.Join(tempDir, filename2))

	dm := NewDiskManager(tempDir)
	indices, err := dm.LoadIndex()
	testutil.MustBeNil(t, err)

	testutil.MustEqual(t, len(indices), 2)

	keys := make([]string, 0, len(indices))
	for key := range indices {
		keys = append(keys, key)
	}
	sort.Strings(keys)

	testutil.MustEqual(t, keys, []string{"items", "user_accounts"})
	testutil.MustEqual(t, indices["user_accounts"], bt)
	testutil.MustEqual(t, indices["items"], bt2)
}

func TestDiskManager_PersistIndex(t *testing.T) {
	tempDir := os.TempDir()
	readIndex := func(t *testing.T, filename string) *btree.BTree {
		file, err := os.OpenFile(path.Join(tempDir, filename), os.O_RDONLY, 0755)
		testutil.MustBeNil(t, err)
		bs, err := io.ReadAll(file)
		testutil.MustBeNil(t, err)
		b, err := btree.Deserialize(bs)
		testutil.MustBeNil(t, err)
		return b
	}
	// testdata 1
	bt := btree.NewIntKeyTree()
	bt.Put(1, "1")

	// testdata 2
	bt2 := btree.NewIntKeyTree()
	bt2.Put(2, "2")

	dm := NewDiskManager(tempDir)

	err := dm.PersistIndex("user_accounts", bt)
	testutil.MustBeNil(t, err)
	defer os.Remove(path.Join(tempDir, "user_accounts.idx"))

	err = dm.PersistIndex("items", bt2)
	testutil.MustBeNil(t, err)
	defer os.Remove(path.Join(tempDir, "items.idx"))

	userAccIdx := readIndex(t, "user_accounts.idx")
	testutil.MustEqual(t, userAccIdx, bt)

	itemsIdx := readIndex(t, "items.idx")
	testutil.MustEqual(t, itemsIdx, bt2)
}

func TestDiskManager_LoadPageDirectory(t *testing.T) {
	tempDir := os.TempDir()
	pd := &PageDirectory{
		PageIDs: map[string][]PageID{
			"items": {PageID(1)},
		},
		PageLocation: map[string]*pageLocation{
			"items#1": {Filename: "items__1.db", Offset: 0},
		},
		MaxPageCountPerFile: 50,
	}

	filename := "__page_directory.db"
	file, err := os.OpenFile(path.Join(tempDir, filename), os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0755)
	testutil.MustBeNil(t, err)

	b, err := json.Marshal(&pd)
	testutil.MustBeNil(t, err)
	file.Write(b)
	defer os.Remove(path.Join(tempDir, filename))

	dm := NewDiskManager(tempDir)
	loaded, err := dm.LoadPageDirectory()
	testutil.MustBeNil(t, err)

	testutil.MustEqual(t, loaded, pd)
}

func TestDiskManager_PersistPageDirectory(t *testing.T) {
	tempDir := os.TempDir()

	pd := &PageDirectory{
		PageIDs: map[string][]PageID{
			"items": {PageID(1)},
		},
		PageLocation: map[string]*pageLocation{
			"items#1": {Filename: "items__1.db", Offset: 0},
		},
		MaxPageCountPerFile: 50,
	}

	filename := "__page_directory.db"

	dm := NewDiskManager(tempDir)

	err := dm.PersistPageDirectory(pd)
	testutil.MustBeNil(t, err)
	defer os.Remove(path.Join(tempDir, filename))

	file, err := os.OpenFile(path.Join(tempDir, filename), os.O_RDONLY, 0755)
	testutil.MustBeNil(t, err)
	bs, err := io.ReadAll(file)
	testutil.MustBeNil(t, err)
	var deserialized PageDirectory
	err = json.Unmarshal(bs, &deserialized)
	testutil.MustBeNil(t, err)

	testutil.MustEqual(t, &deserialized, pd)
}
