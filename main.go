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

	u1 := &ssdb.Tuple{
		Data: []*ssdb.TupleData{
			{Typ: ssdb.Int32, Int32Val: 1},                                        // id
			{Typ: ssdb.Byte64, Byte64Val: [64]byte{'d', 't', 'y', 'l', 'e', 'r'}}, // name
			{Typ: ssdb.Int32, Int32Val: 27},                                       // age
		},
	}

	u2 := &ssdb.Tuple{
		Data: []*ssdb.TupleData{
			{Typ: ssdb.Int32, Int32Val: 2},                                             // id
			{Typ: ssdb.Byte64, Byte64Val: [64]byte{'d', 't', 'y', 'l', 'e', 'r', '2'}}, // name
			{Typ: ssdb.Int32, Int32Val: 28},                                            // age
		},
	}

	insert("users", "users_id", 1, u1)
	insert("users", "users_id", 2, u2)

	if err := engine.Shutdown(); err != nil {
		fmt.Fprintf(os.Stderr, "shutdown engine: %s\n", err)
		return
	}
}
