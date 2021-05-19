package main

import (
	"fmt"
	"os"

	"github.com/dty1er/sdb/engine/ssdb"
)

func main() {
	engine, err := ssdb.New()
	if err != nil {
		fmt.Fprintf(os.Stderr, "init engine: %s\n", err)
		return
	}

	engine.CreateIndex("users_id", ssdb.Int)

	insert := func(table, idxName string, key int, t *ssdb.Tuple) {
		engine.InsertTuple(table, t)
		engine.InsertIndex(idxName, key, t)
	}

	for i := 1; i <= 5; i++ {
		u := &ssdb.Tuple{
			Data: []*ssdb.TupleData{
				{Typ: ssdb.Int32, Int32Val: int32(i)},                                 // id
				{Typ: ssdb.Byte64, Byte64Val: [64]byte{'d', 't', 'y', 'l', 'e', 'r'}}, // name
				{Typ: ssdb.Int32, Int32Val: 27},                                       // age
			},
		}

		insert("users", "users_id", i, u)
	}

	if err := engine.Shutdown(); err != nil {
		fmt.Fprintf(os.Stderr, "shutdown engine: %s\n", err)
		return
	}
}
