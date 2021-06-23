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

## 2021/06/23

Sometimes I need real working database for some testing, and I usually use MySQL and Sakila. Sakila is a sample database provided by MySQL. It's quite easy to use with Docker:

```shell
$ docker run -p 3306:3306 -d sakiladb/mysql:latest
$ mysql -h 127.0.0.1 --port 3306 -u sakila -pp_ssW0rd sakila

# MySQL console starts up and can execute queries...
```

I'm writing tests for Planner Select Plan. I at first thought for Order by query, Projection wraps Order, and Order Wraps another input (e.g. Scan). However, If I do that, it cannot sort the input if some columns for order keys are not projected. For example:

```
SELECT id from users order by name;
```

The execution flow can be `Scan users` -> `Projection id` -> `Order by name`. But the last `Order by name` is impossible because the name column is dropped.
So it seems like I should first order the input, then do the projection. I still don't have enough understanding on SQL execution flow.
