package dal

import (
	"context"
	"github.com/aacfactory/errors"
	"github.com/aacfactory/fns-contrib/databases/sql"
)

func BeginTransaction(ctx context.Context) (err errors.CodeError) {
	err = sql.BeginTransaction(ctx)
	return
}

func CommitTransaction(ctx context.Context) (err errors.CodeError) {
	err = sql.CommitTransaction(ctx)
	return
}

func RollbackTransaction(ctx context.Context) (err errors.CodeError) {
	err = sql.RollbackTransaction(ctx)
	return
}
