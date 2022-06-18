package mysql

import (
	"context"
	"github.com/aacfactory/errors"
	"github.com/aacfactory/fns-contrib/databases/sql"
	"reflect"
)

func Modify(ctx context.Context, row interface{}) (err errors.CodeError) {
	if row == nil {
		err = errors.ServiceError("mysql: modify failed for row is nil").WithMeta("mysql", "modify")
		return
	}
	rv := reflect.ValueOf(row)
	if rv.Type().Kind() != reflect.Ptr {
		err = errors.ServiceError("mysql: modify failed for type of row is not ptr").WithMeta("mysql", "modify")
		return
	}
	if rv.Elem().Type().Kind() != reflect.Struct {
		err = errors.ServiceError("mysql: modify failed for type of row is not ptr struct").WithMeta("mysql", "modify")
		return
	}
	tab := createOrLoadTable(row)
	// modify
	tryFillModifyErr := tryFillAuditModify(ctx, rv, tab)
	if tryFillModifyErr != nil {
		err = errors.ServiceError("mysql: modify failed, try to fill modify audit failed").WithCause(tryFillModifyErr).WithMeta("mysql", "modify")
		return
	}
	// exec
	query := tab.updateQuery.query
	columns := tab.updateQuery.columns
	args, argsErr := mapColumnsToSqlArgs(columns, rv)
	if argsErr != nil {
		err = errors.ServiceError("mysql: modify failed, try to fill args failed").WithCause(argsErr).WithMeta("mysql", "modify")
		return
	}
	affected, _, execErr := sql.Execute(ctx, query, args...)
	if execErr != nil {
		err = errors.ServiceError("mysql: modify failed").WithCause(execErr).WithMeta("mysql", "modify")
		return
	}
	if affected == 0 {
		return
	}
	// version
	tryFillAuditVersion(rv, tab)
	return
}
