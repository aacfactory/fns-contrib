package mysql

import (
	"database/sql"
	"github.com/aacfactory/fns-contrib/databases/sql/dac/conditions"
	"time"
)

func Eq(field string, expression any) conditions.Condition {
	return conditions.New(conditions.Eq(field, expression))
}

func NotEq(field string, expression any) conditions.Condition {
	return conditions.New(conditions.NotEq(field, expression))
}

func Gt(field string, expression any) conditions.Condition {
	return conditions.New(conditions.Gt(field, expression))
}

func Gte(field string, expression any) conditions.Condition {
	return conditions.New(conditions.Gte(field, expression))
}

func Lt(field string, expression any) conditions.Condition {
	return conditions.New(conditions.Lt(field, expression))
}

func Lte(field string, expression any) conditions.Condition {
	return conditions.New(conditions.Lte(field, expression))
}

func Between(field string, left any, right any) conditions.Condition {
	return conditions.New(conditions.Between(field, left, right))
}

func In(field string, expression ...any) conditions.Condition {
	return conditions.New(conditions.In(field, expression...))
}

func NotIn(field string, expression ...any) conditions.Condition {
	return conditions.New(conditions.NotIn(field, expression...))
}

func Like(field string, expression string) conditions.Condition {
	return conditions.New(conditions.Like(field, expression))
}

func LikeLast(field string, expression string) conditions.Condition {
	return conditions.New(conditions.LikeLast(field, expression))
}

func LikeContains(field string, expression string) conditions.Condition {
	return conditions.New(conditions.LikeContains(field, expression))
}

func SubQuery(query any, field string, cond conditions.Condition) conditions.QueryExpr {
	return conditions.Query(query, field, cond)
}

func LitSubQuery(query string) conditions.QueryExpr {
	return conditions.LitQuery(query)
}

func String(s string) conditions.Literal {
	return conditions.String(s)
}

func Bool(b bool) conditions.Literal {
	return conditions.Bool(b)
}

func Int(n int) conditions.Literal {
	return conditions.Int(n)
}

func Int64(n int64) conditions.Literal {
	return conditions.Int64(n)
}

func Float(f float32) conditions.Literal {
	return conditions.Float(f)
}

func Float64(f float64) conditions.Literal {
	return conditions.Float64(f)
}

func Time(t time.Time) conditions.Literal {
	return conditions.Datetime(t)
}

func Lit(v string) conditions.Literal {
	return conditions.Lit(v)
}

func Named(name string, value any) sql.NamedArg {
	return sql.Named(name, value)
}
