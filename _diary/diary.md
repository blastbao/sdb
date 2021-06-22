## 2021/06/22

Started to write this diary. I will write what I considered, did, decided not to do. I will use it to collect my thoughts, to
remember why I implemented xxx. However, it is possibly for other developers who want to write their own RDB.

SDB is a RDB, implemented from scratch without any external libraries. Necessary module should be implemented from scratch,
but it is ok to use Go standard library. The reasons is the main purpose of this project is to learn how database works.

Right now, I am developing planner which creates logical/physical plan for select statement. I already developed simple b-tree,
lru, tokenizer and parser, storage engine... but I feel like the part making execution plan is the hardest.

SQL Select statement is complicated enough. Right now, I am targeting to execute very simple SELECT stmt like below:

```
select id, name from mytable where id = 5 order by name asc limit 5, offset 10;
```

* Projection can be the list of column name or `*`
* Only one table can be specified as FROM
* Single unary operator can be specified as WHERE
* Order By and Limit Offset support

I use [this slide](https://courses.cs.washington.edu/courses/cse444/09sp/lectures/lecture18.pdf) to learn how logical/physical plan
are used.
