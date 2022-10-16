package postgres_test

import (
	"fmt"
	"github.com/aacfactory/fns-contrib/databases/postgres"
	"github.com/aacfactory/json"
	"testing"
	"time"
)

func TestConditions(t *testing.T) {
	conds := postgres.NewConditions(postgres.Eq("id", "FOO"))
	conds.And(postgres.ContainsJsonKey("id", "foo"))
	conds.And(postgres.ContainsJsonObject("id", `{"foo": "bar"}`))
	conds.And(postgres.ContainsJsonObjectOfArray("id", "foo"))
	conds.And(postgres.ContainsJsonObjectsOfArray("id", true, "foo", "bar"))
	conds.And(postgres.ContainsJsonObjectsOfArray("id", false, "foo", "bar"))
	conds.And(postgres.GT("age", 10))
	conds.And(postgres.GTE("age", 11))
	conds.And(postgres.LT("age", 10))
	conds.And(postgres.LTE("age", 11))
	conds.And(postgres.Eq("ref_id", postgres.LitValue(`"schema"."table"."id"`)))
	conds.Or(postgres.Between("time", time.Now(), time.Now()))
	conds.AndConditions(postgres.NewConditions(postgres.Eq("bar", 1.2)).And(postgres.Eq("baz", 11)))
	conds.And(postgres.IN("ids", []int{1, 2, 3, 4}))
	conds.And(postgres.IN("fds", postgres.LitValue(`SELECT "id" from "schema"."table" where "id" > 0`)))

	query, args := conds.QueryAndArguments()
	fmt.Println(query)
	argsP, _ := json.Marshal(args)
	fmt.Println(string(argsP))
}
