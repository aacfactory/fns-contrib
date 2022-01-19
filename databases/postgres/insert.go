package postgres

import (
	"github.com/aacfactory/errors"
	"github.com/aacfactory/fns"
	"github.com/aacfactory/fns-contrib/databases/sql"
	"reflect"
	"time"
)

func Insert(ctx fns.Context, row interface{}) (err errors.CodeError) {
	if row == nil {
		err = errors.ServiceError("fns Postgres: insert failed for row is nil").WithMeta("_fns_postgres", "insert")
		return
	}
	rv := reflect.ValueOf(row)
	if rv.Type().Kind() != reflect.Ptr {
		err = errors.ServiceError("fns Postgres: insert failed for type of row is not ptr").WithMeta("_fns_postgres", "insert")
		return
	}
	if rv.Elem().Type().Kind() != reflect.Struct {
		err = errors.ServiceError("fns Postgres: insert failed for type of row is not ptr struct").WithMeta("_fns_postgres", "insert")
		return
	}
	tab := createOrLoadTable(row)
	// create by
	tryFillCreateErr := tryFillAuditCreate(ctx, rv, tab)
	if tryFillCreateErr != nil {
		err = errors.ServiceError("fns Postgres: insert failed, try to fill create audit failed").WithCause(tryFillCreateErr).WithMeta("_fns_postgres", "insert")
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
		err = errors.ServiceError("fns Postgres: insert failed").WithCause(execErr).WithMeta("_fns_postgres", "insert")
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
		err = errors.ServiceError("fns Postgres: insert or update failed for row is nil").WithMeta("_fns_postgres", "insertOrUpdate")
		return
	}
	rv := reflect.ValueOf(row)
	if rv.Type().Kind() != reflect.Ptr {
		err = errors.ServiceError("fns Postgres: insert or update failed for type of row is not ptr").WithMeta("_fns_postgres", "insertOrUpdate")
		return
	}
	if rv.Elem().Type().Kind() != reflect.Struct {
		err = errors.ServiceError("fns Postgres: insert or update failed for type of row is not ptr struct").WithMeta("_fns_postgres", "insertOrUpdate")
		return
	}
	tab := createOrLoadTable(row)
	querySetting := tab.insertOrUpdateQuery
	if querySetting == nil {
		err = errors.ServiceError("fns Postgres: insert or update failed for type of row is not supported, need conflict or string typed pk").WithMeta("_fns_postgres", "insertOrUpdate")
		return
	}
	// create
	creates := tab.findAuditCreate()
	hasCreates := len(creates) > 0

	if hasCreates {
		createBY := ""
		var createByColumn *column
		createAT := time.Time{}
		var createAtColumn *column
		for _, create := range creates {
			if create.isAcb() {
				createByColumn = create
				createBY = rv.FieldByName(createByColumn.FieldName).Interface().(string)
			}
			if create.isAct() {
				createAtColumn = create
				createAT = rv.FieldByName(createAtColumn.FieldName).Convert(reflect.TypeOf(createAT)).Interface().(time.Time)
			}
		}
		if createByColumn != nil {
			if createBY == "" {
				user := ctx.User()
				if user.Exists() {
					createBY = user.Id()
					rv.FieldByName(createByColumn.FieldName).SetString(createBY)
				}
			}
			if createBY == "" {
				err = errors.ServiceError("fns Postgres: insert failed for create by column value is needed").WithMeta("_fns_postgres", "insert")
				return
			}
		}
		if createAtColumn != nil {
			if createAT.IsZero() {
				createAT = time.Now()
				createAtField := rv.FieldByName(createAtColumn.FieldName)
				createAtField.Set(reflect.ValueOf(createAT).Convert(createAtField.Type()))
			}
		}
	}

	// modify

	// version

	return
}
