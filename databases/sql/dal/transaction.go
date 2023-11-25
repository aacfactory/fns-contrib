package dal

import (
	"github.com/aacfactory/fns-contrib/databases/sql"
	"github.com/aacfactory/fns/context"
)

func BeginTransaction(ctx context.Context) (err error) {
	err = sql.Begin(ctx)
	return
}

func CommitTransaction(ctx context.Context) (err error) {
	err = sql.Commit(ctx)
	return
}

func RollbackTransaction(ctx context.Context) {
	sql.Rollback(ctx)
	return
}
