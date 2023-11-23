package databases

import (
	"context"
	"database/sql"
	"github.com/aacfactory/errors"
	"github.com/aacfactory/fns/services"
)

type Database interface {
	services.Component
	Dialect() (name string)
	BeginTransaction(ctx context.Context) (err errors.CodeError)
	CommitTransaction(ctx context.Context) (finished bool, err errors.CodeError)
	RollbackTransaction(ctx context.Context) (err errors.CodeError)
	Query(ctx context.Context, query string, args []interface{}) (rows *sql.Rows, err errors.CodeError)
	Execute(ctx context.Context, query string, args []interface{}) (result sql.Result, err errors.CodeError)
}
