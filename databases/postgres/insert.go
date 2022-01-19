package postgres

import (
	"github.com/aacfactory/errors"
	"github.com/aacfactory/fns"
	"github.com/aacfactory/fns-contrib/databases/sql"
	"reflect"
)

func Insert(ctx fns.Context, row interface{}) (err errors.CodeError) {
	if row == nil {
		err = errors.ServiceError("fns Postgres: insert failed for row is nil").WithMeta("_fns_postgres", "Insert")
		return
	}
	rv := reflect.ValueOf(row)
	if rv.Type().Kind() != reflect.Ptr {
		err = errors.ServiceError("fns Postgres: insert failed for type of row is not ptr").WithMeta("_fns_postgres", "Insert")
		return
	}
	if rv.Elem().Type().Kind() != reflect.Struct {
		err = errors.ServiceError("fns Postgres: insert failed for type of row is not ptr struct").WithMeta("_fns_postgres", "Insert")
		return
	}
	tab := createOrLoadTable(row)
	// create by
	tryFillCreateErr := tryFillAuditCreate(ctx, rv, tab)
	if tryFillCreateErr != nil {
		err = errors.ServiceError("fns Postgres: insert failed, try to fill create audit failed").WithCause(tryFillCreateErr).WithMeta("_fns_postgres", "Insert")
		return
	}
	// version
	tryFillAuditVersion(rv, tab)
	// exec
	query := tab.insertQuery.query
	columns := tab.insertQuery.columns
	args := sql.NewTuple()
	for _, c := range columns {
		args.Append(rv.FieldByName(c.FieldName).Interface())
	}
	result, execErr := sql.Execute(ctx, sql.Param{
		Query: query,
		Args:  args,
	})
	if execErr != nil {
		err = errors.ServiceError("fns Postgres: insert failed").WithCause(execErr).WithMeta("_fns_postgres", "Insert")
		return
	}
	if result.Affected == 0 {
		return
	}
	// incrPk
	lastInsertId := result.LastInsertId
	if lastInsertId > 0 {
		pks := tab.findPk()
		for _, pk := range pks {
			if pk.isIncrPk() {
				rv.FieldByName(pk.FieldName).SetInt(lastInsertId)
				break
			}
		}
	}
	return
}

func InsertOrUpdate(ctx fns.Context, row interface{}) (err errors.CodeError) {
	if row == nil {
		err = errors.ServiceError("fns Postgres: insert or update failed for row is nil").WithMeta("_fns_postgres", "InsertOrUpdate")
		return
	}
	rv := reflect.ValueOf(row)
	if rv.Type().Kind() != reflect.Ptr {
		err = errors.ServiceError("fns Postgres: insert or update failed for type of row is not ptr").WithMeta("_fns_postgres", "InsertOrUpdate")
		return
	}
	if rv.Elem().Type().Kind() != reflect.Struct {
		err = errors.ServiceError("fns Postgres: insert or update failed for type of row is not ptr struct").WithMeta("_fns_postgres", "InsertOrUpdate")
		return
	}
	tab := createOrLoadTable(row)
	querySetting := tab.insertOrUpdateQuery
	if querySetting == nil {
		err = errors.ServiceError("fns Postgres: insert or update failed for type of row is not supported, need conflict or string typed pk").WithMeta("_fns_postgres", "InsertOrUpdate")
		return
	}
	// create
	tryFillCreateErr := tryFillAuditCreate(ctx, rv, tab)
	if tryFillCreateErr != nil {
		err = errors.ServiceError("fns Postgres: insert or update failed, try to fill create audit failed").WithCause(tryFillCreateErr).WithMeta("_fns_postgres", "InsertOrUpdate")
		return
	}
	// modify

	// version
	tryFillAuditVersion(rv, tab)

	return
}
