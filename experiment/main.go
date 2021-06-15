package main

import (
	"github.com/blastrain/vitess-sqlparser/sqlparser"
	"github.com/k0kubun/pp"
)

func main() {
	// sql := "select distinct items.id, users.name from items left join users on items.id = users.item_id and items.id < 1 where id <= 5 AND id < 10 order by users.id limit 100 offset 5"
	sql := "select * from items where id < 5 order by id desc limit 5 offset 10"
	stmt, err := sqlparser.Parse(sql)
	if err != nil {
		panic(err)
	}

	pp.Println(stmt)
}
