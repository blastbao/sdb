package config

import (
	"bytes"
	"testing"

	"github.com/dty1er/sdb/testutil"
)

func Test_read(t *testing.T) {
	const config = `# comment
[server]
buffer_pool_entry_count = 500

# comment
[client]

[server]
db_files_directory = ./test/
`
	conf := bytes.NewBufferString(config)

	c := defaultConfig
	err := read(conf, c)

	testutil.MustBeNil(t, err)

	testutil.MustEqual(t, c, &Config{
		Server: &Server{BufferPoolEntryCount: 500, DBFilesDirectory: "./test/"},
		Client: &Client{},
	})
}
