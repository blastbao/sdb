package main

import (
	"github.com/blastrain/vitess-sqlparser/sqlparser"
	"github.com/k0kubun/pp"
)

func main() {
	sql := "select id, name from items left join users on items.id = users.item_id where id <= 5 AND id < 10 order by users.id limit 100 offset 5"
	stmt, err := sqlparser.Parse(sql)
	if err != nil {
		panic(err)
	}

	pp.Println(stmt)
}
