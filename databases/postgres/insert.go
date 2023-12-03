package postgres

import (
	"github.com/aacfactory/fns-contrib/databases/postgres/dialect"
	"github.com/aacfactory/fns-contrib/databases/sql"
	"github.com/aacfactory/fns-contrib/databases/sql/dac"
	"github.com/aacfactory/fns-contrib/databases/sql/dac/conditions"
	"github.com/aacfactory/fns/context"
)

func Insert[T Table](ctx context.Context, entry T) (v T, ok bool, err error) {
	sql.ForceDialect(ctx, dialect.Name)
	v, ok, err = dac.Insert[T](ctx, entry)
	return
}

func InsertMulti[T Table](ctx context.Context, entries []T) (affected int64, err error) {
	sql.ForceDialect(ctx, dialect.Name)
	affected, err = dac.InsertMulti[T](ctx, entries)
	return
}

func InsertOrUpdate[T Table](ctx context.Context, entry T) (v T, err error) {
	sql.ForceDialect(ctx, dialect.Name)
	v, err = dac.InsertOrUpdate[T](ctx, entry)
	return
}

func InsertWhenNotExist[T Table](ctx context.Context, entry T, source conditions.QueryExpr) (v T, err error) {
	sql.ForceDialect(ctx, dialect.Name)
	v, err = dac.InsertWhenNotExist[T](ctx, entry, source)
	return
}

func InsertWhenExist[T Table](ctx context.Context, entry T, source conditions.QueryExpr) (v T, err error) {
	sql.ForceDialect(ctx, dialect.Name)
	v, err = dac.InsertWhenExist[T](ctx, entry, source)
	return
}
