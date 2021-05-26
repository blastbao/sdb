package main

import (
	"fmt"
	"os"

	"github.com/dty1er/sdb/cmd/cli"
)

var queries = []string{
	"Create table users (id int64 primary key, name string);",
}

func main() {
	for _, query := range queries {
		resp, err := cli.ExecQuery(query)
		if err != nil {
			fmt.Fprintf(os.Stderr, "%s\n", err)
			continue
		}
		if resp.Result != "OK" {
			fmt.Fprintf(os.Stdout, "execution failed: %s\n", resp.Error.Message)
			continue
		}

		fmt.Fprintf(os.Stdout, "%+v\n", resp.RS)
	}
}
