package postgres

import (
	"context"
	"github.com/aacfactory/errors"
	"github.com/aacfactory/fns-contrib/databases/sql"
	"reflect"
)

func Delete(ctx context.Context, row interface{}) (err errors.CodeError) {
	if row == nil {
		err = errors.ServiceError("postgres: delete failed for row is nil").WithMeta("postgres", "delete")
		return
	}
	rv := reflect.ValueOf(row)
	if rv.Type().Kind() != reflect.Ptr {
		err = errors.ServiceError("postgres: delete failed for type of row is not ptr").WithMeta("postgres", "delete")
		return
	}
	if rv.Elem().Type().Kind() != reflect.Struct {
		err = errors.ServiceError("postgres: delete failed for type of row is not ptr struct").WithMeta("postgres", "delete")
		return
	}
	tab := createOrLoadTable(row)
	// exec
	var genericQuery *tableGenericQuery
	if tab.softDeleteQuery != nil {
		genericQuery = tab.softDeleteQuery
		tryFillDeleteErr := tryFillAuditDelete(ctx, rv, tab)
		if tryFillDeleteErr != nil {
			err = errors.ServiceError("postgres: delete failed, try to fill modify audit failed").WithCause(tryFillDeleteErr).WithMeta("postgres", "delete")
			return
		}
	} else {
		genericQuery = tab.deleteQuery
	}
	query := genericQuery.query
	columns := genericQuery.columns
	args, argsErr := mapColumnsToSqlArgs(columns, rv)
	if argsErr != nil {
		err = errors.ServiceError("postgres: delete failed, try to fill args failed").WithCause(argsErr).WithMeta("postgres", "delete")
		return
	}
	affected, _, execErr := sql.Execute(ctx, query, args...)
	if execErr != nil {
		err = errors.ServiceError("postgres: delete failed").WithCause(execErr).WithMeta("postgres", "delete")
		return
	}
	if affected == 0 {
		return
	}
	// version
	tryFillAuditVersion(rv, tab)
	return
}
