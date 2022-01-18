package postgres_test

import (
	"github.com/aacfactory/fns-contrib/databases/postgres"
	"testing"
)

func TestConditions(t *testing.T) {

	conds := postgres.NewConditions(postgres.Eq("ID", "FOO"))
	conds.And(postgres.GT("age", 10))
	conds.And(postgres.GTE("age", 11))
	conds.And(postgres.GT("age", 10))
	conds.And(postgres.GTE("age", 11))

}
