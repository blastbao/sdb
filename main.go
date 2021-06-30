package main

import (
	"fmt"
	"os"

	"github.com/dty1er/sdb/cmd/cli"
)

var queries = []string{
	"Create table users (id int64 primary key, name string, verified bool, registered timestamp, height float64);",
	`insert into users values (1, "user1", true, "2021-06-01 10:00:00", 175.3);`,
	`insert into users values (2, "user2", false, "2021-06-01 11:00:00", 175.0);`,
	`insert into users values (3, "user3", true, "2021-06-01 12:00:00", 175.3);`,
	`select * from users;`,
}

func main() {
	for _, query := range queries {
		resp, err := cli.ExecQuery("http://localhost:5525", query)
		if err != nil {
			fmt.Fprintf(os.Stderr, "%s\n", err)
			continue
		}
		if resp.Code != "OK" {
			fmt.Fprintf(os.Stdout, "execution failed: %s\n", resp.Error.Message)
			continue
		}

		fmt.Fprintf(os.Stdout, "%+v\n", resp.RS)
	}
}
