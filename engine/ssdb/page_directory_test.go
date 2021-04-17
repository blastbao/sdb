package ssdb

import (
	"os"
	"path"
	"reflect"
	"testing"
)

func TestEncodePageDirectoryID(t *testing.T) {
	given := EncodePageDirectoryID("users", PageID(5))
	expected := "users#5"
	if string(given) != expected {
		t.Errorf("unexpected: %v", given)
	}
}

func TestPageDirectory_GetPageIDs(t *testing.T) {
	pd := &PageDirectory{
		pageIDs: map[string][]PageID{"users": {PageID(1), PageID(3), PageID(5)}},
	}

	ids := pd.GetPageIDs("items")
	if len(ids) != 0 {
		t.Errorf("unexpected length: %d", len(ids))
	}

	ids = pd.GetPageIDs("users")
	if len(ids) != 3 {
		t.Errorf("unexpected length: %d", len(ids))
	}

	if !reflect.DeepEqual(ids, []PageID{PageID(1), PageID(3), PageID(5)}) {
		t.Errorf("unexpected: %v", ids)
	}
}

func TestPageDirectory_GetPageContent(t *testing.T) {
	tempdir := os.TempDir()

	// prepare test data
	page1 := NewPage(1)
	page1.AppendTuple(Tuple{
		Data: []TupleData{
			{Typ: Int32, Int32Val: 1},
			{Typ: Byte64, Byte64Val: [64]byte{'a', 'b', 'c'}},
		},
	})

	page2 := NewPage(2)
	page2.AppendTuple(Tuple{
		Data: []TupleData{
			{Typ: Int32, Int32Val: 2},
			{Typ: Byte64, Byte64Val: [64]byte{'a', 'b', 'c'}},
		},
	})

	page3 := NewPage(3)
	page3.AppendTuple(Tuple{
		Data: []TupleData{
			{Typ: Int32, Int32Val: 3},
			{Typ: Byte64, Byte64Val: [64]byte{'a', 'b', 'c'}},
		},
	})

	usersF, err := os.OpenFile(path.Join(tempdir, "users_1.db"), os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0666)
	if err != nil {
		t.Errorf("unexpected error: %s", err)
	}
	users2F, err := os.OpenFile(path.Join(tempdir, "users_2.db"), os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0666)
	if err != nil {
		t.Errorf("unexpected error: %s", err)
	}

	usersF.Write(page1.bs[:])
	usersF.Write(page2.bs[:])
	users2F.Write(page3.bs[:])

	defer os.Remove(path.Join(tempdir, "users_1.db"))
	defer os.Remove(path.Join(tempdir, "users_2.db"))

	// build page directory by prepared data
	pd := &PageDirectory{
		pageLocation: map[PageDirectoryID]pageLocation{
			PageDirectoryID("users#1"): {filename: path.Join(tempdir, "users_1.db"), offset: 0},
			PageDirectoryID("users#2"): {filename: path.Join(tempdir, "users_1.db"), offset: PageSize},
			PageDirectoryID("users#3"): {filename: path.Join(tempdir, "users_2.db"), offset: 0},
		},
	}

	p1, err := pd.GetPageContent("users", PageID(1))
	if err != nil {
		t.Errorf("unexpected error: %s", err)
	}

	if !reflect.DeepEqual(p1, page1) {
		t.Errorf("unexpected page: %v", p1)
	}

	p2, err := pd.GetPageContent("users", PageID(2))
	if err != nil {
		t.Errorf("unexpected error: %s", err)
	}

	if !reflect.DeepEqual(p2, page2) {
		t.Errorf("unexpected page: %v", p1)
	}

	p3, err := pd.GetPageContent("users", PageID(3))
	if err != nil {
		t.Errorf("unexpected error: %s", err)
	}

	if !reflect.DeepEqual(p3, page3) {
		t.Errorf("unexpected page: %v", p1)
	}
}
