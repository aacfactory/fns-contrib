package dac

import (
	"github.com/aacfactory/fns-contrib/databases/sql"
	"github.com/aacfactory/fns-contrib/databases/sql/databases"
	"github.com/aacfactory/fns/context"
)

func Begin(ctx context.Context, options ...databases.TransactionOption) (err error) {
	err = sql.Begin(ctx, options...)
	return
}

func Commit(ctx context.Context) (err error) {
	err = sql.Commit(ctx)
	return
}

func Rollback(ctx context.Context) {
	sql.Rollback(ctx)
	return
}
