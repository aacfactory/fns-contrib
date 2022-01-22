package postgres

import (
	"fmt"
	"github.com/aacfactory/errors"
	"github.com/aacfactory/fns"
	"github.com/aacfactory/fns-contrib/databases/sql"
	"reflect"
	"strings"
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
	// exec
	query := tab.insertQuery.query
	columns := tab.insertQuery.columns
	args := sql.NewTuple()
	argsErr := mapColumnsToSqlArgs(columns, rv, args)
	if argsErr != nil {
		err = errors.ServiceError("fns Postgres: insert failed, try to fill args failed").WithCause(argsErr).WithMeta("_fns_postgres", "Insert")
		return
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
	// version
	tryFillAuditVersionExact(rv, tab, int64(1))
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
	tryFillModifyErr := tryFillAuditModify(ctx, rv, tab)
	if tryFillModifyErr != nil {
		err = errors.ServiceError("fns Postgres: insert or update failed, try to fill modify audit failed").WithCause(tryFillModifyErr).WithMeta("_fns_postgres", "InsertOrUpdate")
		return
	}
	// exec
	query := querySetting.query
	columns := querySetting.columns
	args := sql.NewTuple()
	argsErr := mapColumnsToSqlArgs(columns, rv, args)
	if argsErr != nil {
		err = errors.ServiceError("fns Postgres: insert or update failed, try to fill args failed").WithCause(argsErr).WithMeta("_fns_postgres", "InsertOrUpdate")
		return
	}
	result, execErr := sql.Execute(ctx, sql.Param{
		Query: query,
		Args:  args,
	})
	if execErr != nil {
		err = errors.ServiceError("fns Postgres: insert or update failed").WithCause(execErr).WithMeta("_fns_postgres", "InsertOrUpdate")
		return
	}
	if result.Affected == 0 {
		return
	}
	// version
	tryFillAuditVersion(rv, tab)
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

func InsertWhenExist(ctx fns.Context, row interface{}, source Select) (err errors.CodeError) {
	execErr := insertWhenExistOrNot(ctx, row, true, source)
	if execErr != nil {
		err = errors.ServiceError("fns Postgres: insert when exist failed").WithCause(execErr).WithMeta("_fns_postgres", "InsertWhenExist")
		return
	}
	return
}

func InsertWhenNotExist(ctx fns.Context, row interface{}, source Select) (err errors.CodeError) {
	execErr := insertWhenExistOrNot(ctx, row, false, source)
	if execErr != nil {
		err = errors.ServiceError("fns Postgres: insert when not exist failed").WithCause(execErr).WithMeta("_fns_postgres", "InsertWhenNotExist")
		return
	}
	return
}

func insertWhenExistOrNot(ctx fns.Context, row interface{}, exist bool, source Select) (err error) {
	if row == nil {
		err = fmt.Errorf("row is nil")
		return
	}
	if source == nil {
		err = fmt.Errorf("source is nil")
		return
	}
	rv := reflect.ValueOf(row)
	if rv.Type().Kind() != reflect.Ptr {
		err = fmt.Errorf("type of row is not ptr")
		return
	}
	if rv.Elem().Type().Kind() != reflect.Struct {
		err = fmt.Errorf("type of row is not ptr struct")
		return
	}
	tab := createOrLoadTable(row)
	// create by
	tryFillCreateErr := tryFillAuditCreate(ctx, rv, tab)
	if tryFillCreateErr != nil {
		err = tryFillCreateErr
		return
	}
	// exec
	query := tab.insertWhenExistOrNotQuery.query
	columns := tab.insertWhenExistOrNotQuery.columns
	args := sql.NewTuple()
	argsErr := mapColumnsToSqlArgs(columns, rv, args)
	if argsErr != nil {
		err = fmt.Errorf("try to fill args failed, %v", argsErr)
		return
	}
	sourceQuery := source.Build(args)
	if exist {
		query = strings.Replace(query, "$$EXISTS$$", "EXISTS", 1)
	} else {
		query = strings.Replace(query, "$$EXISTS$$", "NOT EXISTS", 1)
	}
	query = strings.Replace(query, "$$SOURCE_QUERY$$", sourceQuery, 1)
	result, execErr := sql.Execute(ctx, sql.Param{
		Query: query,
		Args:  args,
	})
	if execErr != nil {
		err = execErr
		return
	}
	if result.Affected == 0 {
		return
	}
	// version
	tryFillAuditVersionExact(rv, tab, int64(1))
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
