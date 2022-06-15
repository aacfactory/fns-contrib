package postgres

import (
	"context"
	"github.com/aacfactory/errors"
	"github.com/aacfactory/fns-contrib/databases/sql"
	"reflect"
)

func Modify(ctx context.Context, row interface{}) (err errors.CodeError) {
	if row == nil {
		err = errors.ServiceError("postgres: modify failed for row is nil").WithMeta("postgres", "modify")
		return
	}
	rv := reflect.ValueOf(row)
	if rv.Type().Kind() != reflect.Ptr {
		err = errors.ServiceError("postgres: modify failed for type of row is not ptr").WithMeta("postgres", "modify")
		return
	}
	if rv.Elem().Type().Kind() != reflect.Struct {
		err = errors.ServiceError("postgres: modify failed for type of row is not ptr struct").WithMeta("postgres", "modify")
		return
	}
	tab := createOrLoadTable(row)
	// modify
	tryFillModifyErr := tryFillAuditModify(ctx, rv, tab)
	if tryFillModifyErr != nil {
		err = errors.ServiceError("postgres: modify failed, try to fill modify audit failed").WithCause(tryFillModifyErr).WithMeta("postgres", "modify")
		return
	}
	// exec
	query := tab.updateQuery.query
	columns := tab.updateQuery.columns
	args, argsErr := mapColumnsToSqlArgs(columns, rv)
	if argsErr != nil {
		err = errors.ServiceError("postgres: modify failed, try to fill args failed").WithCause(argsErr).WithMeta("postgres", "modify")
		return
	}
	affected, _, execErr := sql.Execute(ctx, query, args...)
	if execErr != nil {
		err = errors.ServiceError("postgres: modify failed").WithCause(execErr).WithMeta("postgres", "modify")
		return
	}
	if affected == 0 {
		return
	}
	// version
	tryFillAuditVersion(rv, tab)
	return
}
