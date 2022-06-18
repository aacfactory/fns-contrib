package mysql_test

import (
	"fmt"
	"github.com/aacfactory/fns-contrib/databases/mysql"
	"github.com/aacfactory/json"
	"testing"
	"time"
)

func TestConditions(t *testing.T) {
	conds := mysql.NewConditions(mysql.Eq("id", "FOO"))
	conds.And(mysql.GT("age", 10))
	conds.And(mysql.GTE("age", 11))
	conds.And(mysql.LT("age", 10))
	conds.And(mysql.LTE("age", 11))
	conds.And(mysql.Eq("ref_id", mysql.LitValue(`"schema"."table"."id"`)))
	conds.Or(mysql.Between("time", time.Now(), time.Now()))
	conds.AndConditions(mysql.NewConditions(mysql.Eq("bar", 1.2)).And(mysql.Eq("baz", 11)))
	conds.And(mysql.IN("ids", []int{1, 2, 3, 4}))
	conds.And(mysql.IN("fds", mysql.LitValue(`SELECT "id" from "schema"."table" where "id" > 0`)))

	query, args := conds.QueryAndArguments()
	fmt.Println(query)
	argsP, _ := json.Marshal(args)
	fmt.Println(string(argsP))
}
