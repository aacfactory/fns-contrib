package postgres

import (
	"github.com/aacfactory/errors"
	"github.com/aacfactory/fns"
	"github.com/aacfactory/fns-contrib/databases/sql"
	"reflect"
)

func Delete(ctx fns.Context, row interface{}) (err errors.CodeError) {
	if row == nil {
		err = errors.ServiceError("fns Postgres: delete failed for row is nil").WithMeta("_fns_postgres", "Delete")
		return
	}
	rv := reflect.ValueOf(row)
	if rv.Type().Kind() != reflect.Ptr {
		err = errors.ServiceError("fns Postgres: delete failed for type of row is not ptr").WithMeta("_fns_postgres", "Delete")
		return
	}
	if rv.Elem().Type().Kind() != reflect.Struct {
		err = errors.ServiceError("fns Postgres: delete failed for type of row is not ptr struct").WithMeta("_fns_postgres", "Delete")
		return
	}
	tab := createOrLoadTable(row)
	// exec
	var genericQuery *tableGenericQuery
	if tab.softDeleteQuery != nil {
		genericQuery = tab.softDeleteQuery
		tryFillModifyErr := tryFillAuditDelete(ctx, rv, tab)
		if tryFillModifyErr != nil {
			err = errors.ServiceError("fns Postgres: delete failed, try to fill modify audit failed").WithCause(tryFillModifyErr).WithMeta("_fns_postgres", "Delete")
			return
		}
	} else {
		genericQuery = tab.deleteQuery
	}
	query := genericQuery.query
	columns := genericQuery.columns
	args := sql.NewTuple()
	for _, c := range columns {
		args.Append(rv.FieldByName(c.FieldName).Interface())
	}
	result, execErr := sql.Execute(ctx, sql.Param{
		Query: query,
		Args:  args,
	})
	if execErr != nil {
		err = errors.ServiceError("fns Postgres: delete failed").WithCause(execErr).WithMeta("_fns_postgres", "Delete")
		return
	}
	if result.Affected == 0 {
		return
	}
	// version
	tryFillAuditVersion(rv, tab)
	return
}
