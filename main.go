package main

import (
	"fmt"
	"os"

	"github.com/dty1er/sdb/engine"
)

func main() {
	e, err := engine.New()
	if err != nil {
		fmt.Fprintf(os.Stderr, "init engine: %s\n", err)
		return
	}

	e.CreateIndex("users_id")

	insert := func(table, idxName string, t *engine.Tuple) {
		e.InsertTuple(table, t)
		e.InsertIndex(idxName, t)
	}

	for i := 1; i <= 5; i++ {
		u := &engine.Tuple{
			Data: []*engine.TupleData{
				{Typ: engine.Int32, Int32Val: int32(i), Key: true},                      // id
				{Typ: engine.Byte64, Byte64Val: [64]byte{'d', 't', 'y', 'l', 'e', 'r'}}, // name
				{Typ: engine.Int32, Int32Val: 27},                                       // age
			},
		}

		insert("users", "users_id", u)
	}

	if err := e.Shutdown(); err != nil {
		fmt.Fprintf(os.Stderr, "shutdown engine: %s\n", err)
		return
	}
}
