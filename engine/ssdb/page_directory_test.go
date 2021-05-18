package ssdb

import (
	"os"
	"path"
	"reflect"
	"testing"

	"github.com/dty1er/sdb/testutil"
)

func TestEncodePageDirectoryID(t *testing.T) {
	given := EncodePageDirectoryID("users", PageID(5))
	expected := "users#5"
	if string(given) != expected {
		t.Errorf("unexpected: %v", given)
	}
}

func Test_Save_Load_PageDirectory(t *testing.T) {
	tempDir := os.TempDir()

	// Remove the file in advance, just in case
	os.Remove(path.Join(tempDir, "__page_directory.db"))

	defer os.Remove(path.Join(tempDir, "__page_directory.db"))

	// when no file exists, empty page directory will be responded
	loaded, err := LoadPageDirectory(tempDir)
	testutil.MustBeNil(t, err)
	testutil.MustEqual(t, loaded, &PageDirectory{
		PageIDs:             map[string][]PageID{},
		PageLocation:        map[string]*pageLocation{},
		MaxPageCountPerFile: 1000,
	})

	pd := &PageDirectory{
		PageIDs: map[string][]PageID{
			"users": {PageID(1), PageID(3), PageID(5)},
		},
		PageLocation: map[string]*pageLocation{
			"users#1": {Filename: "/tmp/users__1.db"},
			"users#3": {Filename: "/tmp/users__1.db"},
			"users#5": {Filename: "/tmp/users__1.db"},
		},
		MaxPageCountPerFile: 1000,
	}

	err = pd.Save(tempDir)
	testutil.MustBeNil(t, err)

	loaded, err = LoadPageDirectory(tempDir)
	testutil.MustBeNil(t, err)
	testutil.MustEqual(t, loaded, pd)
}

func TestPageDirectory_GetPageIDs(t *testing.T) {
	pd := &PageDirectory{
		PageIDs: map[string][]PageID{"users": {PageID(1), PageID(3), PageID(5)}},
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

func TestPageDirectory_RegisterPage(t *testing.T) {
	tests := []struct {
		name                string
		pageIDs             map[string][]PageID
		pageLocation        map[string]*pageLocation
		maxPageCountPerFile int
		table               string
		page                *Page

		wantPageIDs      map[string][]PageID
		wantPageLocation map[string]*pageLocation
	}{
		{
			name: "no pages exist for the table",
			pageIDs: map[string][]PageID{
				"items": {PageID(1)},
			},
			pageLocation: map[string]*pageLocation{
				"items#1": {Filename: "items__1.db", Offset: 0},
			},
			maxPageCountPerFile: 50,
			table:               "users",
			page:                NewPage(1),

			wantPageIDs: map[string][]PageID{
				"items": {PageID(1)},
				"users": {PageID(1)},
			},
			wantPageLocation: map[string]*pageLocation{
				"items#1": {Filename: "items__1.db", Offset: 0},
				"users#1": {Filename: "users__1.db", Offset: 0},
			},
		},
		{
			name: "the page is appended to the last file",
			pageIDs: map[string][]PageID{
				"items": {PageID(1), PageID(2), PageID(3)},
			},
			pageLocation: map[string]*pageLocation{
				"items#1": {Filename: "items__1.db", Offset: 0},
				"items#2": {Filename: "items__1.db", Offset: PageSize},
				"items#3": {Filename: "items__1.db", Offset: PageSize * 2},
			},
			maxPageCountPerFile: 10,
			table:               "items",
			page:                NewPage(4),

			wantPageIDs: map[string][]PageID{
				"items": {PageID(1), PageID(2), PageID(3), PageID(4)},
			},
			wantPageLocation: map[string]*pageLocation{
				"items#1": {Filename: "items__1.db", Offset: 0},
				"items#2": {Filename: "items__1.db", Offset: PageSize},
				"items#3": {Filename: "items__1.db", Offset: PageSize * 2},
				"items#4": {Filename: "items__1.db", Offset: PageSize * 3},
			},
		},
		{
			name: "the page is appended to the new file because the file contains enough pages",
			pageIDs: map[string][]PageID{
				"items": {PageID(1), PageID(2), PageID(3)},
			},
			pageLocation: map[string]*pageLocation{
				"items#1": {Filename: "items__1.db", Offset: 0},
				"items#2": {Filename: "items__1.db", Offset: PageSize},
				"items#3": {Filename: "items__1.db", Offset: PageSize * 2},
			},
			maxPageCountPerFile: 3, // because 1 page should have 3 pages, new file will be added
			table:               "items",
			page:                NewPage(4),

			wantPageIDs: map[string][]PageID{
				"items": {PageID(1), PageID(2), PageID(3), PageID(4)},
			},
			wantPageLocation: map[string]*pageLocation{
				"items#1": {Filename: "items__1.db", Offset: 0},
				"items#2": {Filename: "items__1.db", Offset: PageSize},
				"items#3": {Filename: "items__1.db", Offset: PageSize * 2},
				"items#4": {Filename: "items__2.db", Offset: 0},
			},
		},
	}

	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			pd := &PageDirectory{
				PageIDs:             test.pageIDs,
				PageLocation:        test.pageLocation,
				MaxPageCountPerFile: test.maxPageCountPerFile,
			}

			pd.RegisterPage(test.table, test.page)
			testutil.MustEqual(t, pd.PageIDs, test.wantPageIDs)
			testutil.MustEqual(t, pd.PageLocation, test.wantPageLocation)
		})
	}
}

func TestPageDirectory_GetPageLocation(t *testing.T) {
	locations := []*pageLocation{
		{Filename: "/tmp/users__1.db", Offset: 0},
		{Filename: "/tmp/users__1.db", Offset: PageSize},
		{Filename: "/tmp/users__2.db", Offset: 0},
	}

	// build page directory by prepared data
	pd := &PageDirectory{
		PageLocation: map[string]*pageLocation{
			"users#1": locations[0],
			"users#2": locations[1],
			"users#3": locations[2],
		},
	}

	p1, err := pd.GetPageLocation("users", PageID(1))
	if err != nil {
		t.Errorf("unexpected error: %s", err)
	}

	if !reflect.DeepEqual(p1, locations[0]) {
		t.Errorf("unexpected page: %v", p1)
	}

	p2, err := pd.GetPageLocation("users", PageID(2))
	if err != nil {
		t.Errorf("unexpected error: %s", err)
	}

	if !reflect.DeepEqual(p2, locations[1]) {
		t.Errorf("unexpected page: %v", p2)
	}

	p3, err := pd.GetPageLocation("users", PageID(3))
	if err != nil {
		t.Errorf("unexpected error: %s", err)
	}

	if !reflect.DeepEqual(p3, locations[2]) {
		t.Errorf("unexpected page: %v", p1)
	}
}

func Test_toFilename_fileInfoFromFilename(t *testing.T) {
	filename := toFilename("user_accounts", 3)
	if filename != "user_accounts__3.db" {
		t.Errorf("unexpected filename: %s", filename)
	}

	table, offset := fileInfofromFilename(filename)
	if table != "user_accounts" {
		t.Errorf("unexpected table: %s", table)
	}
	if offset != 3 {
		t.Errorf("unexpected offset: %d", offset)
	}
}
