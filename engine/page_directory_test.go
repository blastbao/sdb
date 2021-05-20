package engine

import (
	"testing"

	"github.com/dty1er/sdb/testutil"
)

func TestEncodePageDirectoryID(t *testing.T) {
	given := EncodePageDirectoryID("users", PageID(5))
	expected := "users#5"
	testutil.MustEqual(t, string(given), expected)
}

func TestPageDirectory_GetPageIDs(t *testing.T) {
	pd := &PageDirectory{
		PageIDs: map[string][]PageID{"users": {PageID(1), PageID(3), PageID(5)}},
	}

	ids := pd.GetPageIDs("items")
	testutil.MustEqual(t, len(ids), 0)

	ids = pd.GetPageIDs("users")
	testutil.MustEqual(t, len(ids), 3)
	testutil.MustEqual(t, ids, []PageID{PageID(1), PageID(3), PageID(5)})
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
			page:                InitPage(1),

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
			page:                InitPage(4),

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
			page:                InitPage(4),

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
	testutil.MustBeNil(t, err)
	testutil.MustEqual(t, p1, locations[0])

	p2, err := pd.GetPageLocation("users", PageID(2))
	testutil.MustBeNil(t, err)
	testutil.MustEqual(t, p2, locations[1])

	p3, err := pd.GetPageLocation("users", PageID(3))
	testutil.MustBeNil(t, err)
	testutil.MustEqual(t, p3, locations[2])
}

func Test_toFilename_fileInfoFromFilename(t *testing.T) {
	filename := toFilename("user_accounts", 3)
	testutil.MustEqual(t, filename, "user_accounts__3.db")

	table, offset := fileInfofromFilename(filename)
	testutil.MustEqual(t, table, "user_accounts")
	testutil.MustEqual(t, offset, 3)
}
