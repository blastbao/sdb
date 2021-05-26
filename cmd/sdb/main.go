package main

import (
	"fmt"
	"os"

	"github.com/dty1er/sdb/cmd/cli"
)

func main() {
	if err := cli.Run(os.Args[1:]); err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
}
