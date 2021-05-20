package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"os"
	"path"
	"strings"

	"github.com/dty1er/sdb/btree"
	"github.com/dty1er/sdb/engine"
)

type DebugCommand struct {
	fs     *flag.FlagSet
	target string

	// for showIndex
	idxName string

	// for showPage
	pageDescriptorID string
}

func NewDebugCommand() *DebugCommand {
	dc := &DebugCommand{
		fs: flag.NewFlagSet("debug", flag.ExitOnError),
	}

	dc.fs.StringVar(&dc.target, "target", "pd", "debug target")
	dc.fs.StringVar(&dc.idxName, "idxName", "", "index name")
	dc.fs.StringVar(&dc.pageDescriptorID, "pdid", "", "page descriptor id")

	return dc
}

func (dc *DebugCommand) Name() string {
	return dc.fs.Name()
}

func (dc *DebugCommand) Init(args []string) error {
	return dc.fs.Parse(args)
}

func (dc *DebugCommand) Run() error {
	switch dc.target {
	case "pd", "page_directory":
		return dc.showPageDirectory()
	case "idx", "index":
		return dc.showIndex()
	case "pg", "page":
		return dc.showPage()
	default:
		return nil
	}
}

func (dc *DebugCommand) showPageDirectory() error {
	filename := path.Join("./db", "__page_directory.db")
	if _, err := os.Stat(filename); err != nil {
		return fmt.Errorf("page directory file does not exist")
	}

	file, err := os.OpenFile(filename, os.O_RDONLY, 0755)
	if err != nil {
		return fmt.Errorf("open file %s, %w", filename, err)
	}

	var pd engine.PageDirectory
	if err := json.NewDecoder(file).Decode(&pd); err != nil {
		return fmt.Errorf("deserialize json file %s, %w", filename, err)
	}

	fmt.Printf("=======Debug: PageDirectory (%s)\n", filename)
	fmt.Println(&pd)
	fmt.Printf("=======\n")
	return nil
}

func (dc *DebugCommand) showIndex() error {
	if dc.idxName == "" {
		return fmt.Errorf("idxName must be specified")
	}
	filename := path.Join("./db", fmt.Sprintf("%s.idx", dc.idxName))
	if _, err := os.Stat(filename); err != nil {
		return fmt.Errorf("index file does not exist")
	}

	file, err := os.OpenFile(filename, os.O_RDONLY, 0755)
	if err != nil {
		return fmt.Errorf("open file %s, %w", filename, err)
	}

	bs, err := io.ReadAll(file)
	if err != nil {
		return fmt.Errorf("read file %s, %w", filename, err)
	}

	deserialized, err := btree.Deserialize(bs)
	if err != nil {
		return fmt.Errorf("deserialize index %s, %w", filename, err)
	}

	fmt.Printf("=======Debug: Index (%s)\n", filename)
	fmt.Println(deserialized)
	fmt.Printf("=======\n")
	return nil
}

func (dc *DebugCommand) showPage() error {
	if dc.pageDescriptorID == "" {
		return fmt.Errorf("pageDescriptor ID must be specified")
	}

	table := strings.Split(dc.pageDescriptorID, "__")[0]

	// first, read page directory to know how many pages are in the file
	pdFilename := path.Join("./db", "__page_directory.db")
	if _, err := os.Stat(pdFilename); err != nil {
		return fmt.Errorf("page directory file does not exist")
	}

	file, err := os.OpenFile(pdFilename, os.O_RDONLY, 0755)
	if err != nil {
		return fmt.Errorf("open file %s, %w", pdFilename, err)
	}

	var pd engine.PageDirectory
	if err := json.NewDecoder(file).Decode(&pd); err != nil {
		return fmt.Errorf("deserialize json file %s, %w", pdFilename, err)
	}

	// then, read page file
	pgFilename := path.Join("./db", fmt.Sprintf("%s.db", dc.pageDescriptorID))
	if _, err := os.Stat(pgFilename); err != nil {
		return fmt.Errorf("page file does not exist")
	}

	pgFile, err := os.OpenFile(pgFilename, os.O_RDONLY, 0755)
	if err != nil {
		return fmt.Errorf("open file %s, %w", pgFilename, err)
	}

	pagesCount := len(pd.GetPageIDs(table))

	for i := 0; i < pagesCount; i++ {
		bs := [engine.PageSize]byte{}
		_, err = pgFile.ReadAt(bs[:], int64(i*engine.PageSize))
		if err != nil {
			return fmt.Errorf("read file %s, %w", pgFilename, err)
		}

		p := engine.NewPage(bs)

		fmt.Printf("=======Debug: Page (%s)\n", pgFilename)
		fmt.Println(p)
		fmt.Printf("=======\n")
	}
	return nil
}
