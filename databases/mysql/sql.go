package mysql

import (
	"context"
	"github.com/aacfactory/errors"
	"github.com/aacfactory/fns-contrib/databases/sql"
)

func BeginTransaction(ctx context.Context) (err errors.CodeError) {
	err = sql.BeginTransaction(ctx)
	if err != nil {
		err = errors.ServiceError("mysql: begin transaction failed").WithCause(err)
		return
	}
	return
}

func CommitTransaction(ctx context.Context) (err errors.CodeError) {
	err = sql.CommitTransaction(ctx)
	if err != nil {
		err = errors.ServiceError("mysql: commit transaction failed").WithCause(err)
		return
	}
	return
}

func RollbackTransaction(ctx context.Context) (err errors.CodeError) {
	err = sql.RollbackTransaction(ctx)
	if err != nil {
		err = errors.ServiceError("mysql: rollback transaction failed").WithCause(err)
		return
	}
	return
}

func QueryContext(ctx context.Context, query string, args ...interface{}) (rows *Rows, err errors.CodeError) {
	results, queryErr := sql.Query(ctx, query, args...)
	if queryErr != nil {
		err = errors.ServiceError("mysql: query failed").WithCause(queryErr).WithMeta("mysql", "query context").WithMeta("query", query)
		return
	}
	rows = &Rows{
		value: results,
	}
	return
}

func ExecuteContext(ctx context.Context, query string, args ...interface{}) (affected int64, lastInsertId int64, err errors.CodeError) {
	affected, lastInsertId, err = sql.Execute(ctx, query, args...)
	if err != nil {
		err = errors.ServiceError("mysql: execute failed").WithCause(err).WithMeta("mysql", "execute context").WithMeta("query", query)
		return
	}
	return
}

func SwitchDatabase(ctx context.Context, dbname string) context.Context {
	return context.WithValue(ctx, "_sql_dbname", dbname)
}
