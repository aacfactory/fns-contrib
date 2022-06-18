package mysql

import (
	"context"
	"fmt"
	"github.com/aacfactory/errors"
	"github.com/aacfactory/fns-contrib/databases/sql"
	"reflect"
	"strings"
)

func Insert(ctx context.Context, row interface{}) (err errors.CodeError) {
	if row == nil {
		err = errors.ServiceError("mysql: insert failed for row is nil").WithMeta("mysql", "insert")
		return
	}
	rv := reflect.ValueOf(row)
	if rv.Type().Kind() != reflect.Ptr {
		err = errors.ServiceError("mysql: insert failed for type of row is not ptr").WithMeta("mysql", "insert")
		return
	}
	if rv.Elem().Type().Kind() != reflect.Struct {
		err = errors.ServiceError("mysql: insert failed for type of row is not ptr struct").WithMeta("mysql", "insert")
		return
	}
	tab := createOrLoadTable(row)
	// create by
	tryFillCreateErr := tryFillAuditCreate(ctx, rv, tab)
	if tryFillCreateErr != nil {
		err = errors.ServiceError("mysql: insert failed, try to fill create audit failed").WithCause(tryFillCreateErr).WithMeta("mysql", "insert")
		return
	}
	// exec
	query := tab.insertQuery.query
	columns := tab.insertQuery.columns
	args, argsErr := mapColumnsToSqlArgs(columns, rv)
	if argsErr != nil {
		err = errors.ServiceError("mysql: insert failed, try to fill args failed").WithCause(argsErr).WithMeta("mysql", "insert")
		return
	}
	affected, lastInsertId, execErr := sql.Execute(ctx, query, args...)
	if execErr != nil {
		err = errors.ServiceError("mysql: insert failed").WithCause(execErr).WithMeta("mysql", "insert")
		return
	}
	if affected == 0 {
		return
	}
	// incrPk
	if lastInsertId > 0 {
		pks := tab.findPk()
		for _, pk := range pks {
			if pk.isIncrPk() {
				rv.Elem().FieldByName(pk.FieldName).SetInt(lastInsertId)
				break
			}
		}
	}
	// version
	tryFillAuditVersionExact(rv, tab, int64(1))
	return
}

func InsertOrUpdate(ctx context.Context, row interface{}) (err errors.CodeError) {
	if row == nil {
		err = errors.ServiceError("mysql: insert or update failed for row is nil").WithMeta("mysql", "insert or update")
		return
	}
	rv := reflect.ValueOf(row)
	if rv.Type().Kind() != reflect.Ptr {
		err = errors.ServiceError("mysql: insert or update failed for type of row is not ptr").WithMeta("mysql", "insert or update")
		return
	}
	if rv.Elem().Type().Kind() != reflect.Struct {
		err = errors.ServiceError("mysql: insert or update failed for type of row is not ptr struct").WithMeta("mysql", "insert or update")
		return
	}
	tab := createOrLoadTable(row)
	querySetting := tab.insertOrUpdateQuery
	if querySetting == nil {
		err = errors.ServiceError("mysql: insert or update failed for type of row is not supported, need conflict or string typed pk").WithMeta("mysql", "insert or update")
		return
	}
	// create
	tryFillCreateErr := tryFillAuditCreate(ctx, rv, tab)
	if tryFillCreateErr != nil {
		err = errors.ServiceError("mysql: insert or update failed, try to fill create audit failed").WithCause(tryFillCreateErr).WithMeta("mysql", "insert or update")
		return
	}
	// modify
	tryFillModifyErr := tryFillAuditModify(ctx, rv, tab)
	if tryFillModifyErr != nil {
		err = errors.ServiceError("mysql: insert or update failed, try to fill modify audit failed").WithCause(tryFillModifyErr).WithMeta("mysql", "insert or update")
		return
	}
	// exec
	query := querySetting.query
	columns := querySetting.columns
	args, argsErr := mapColumnsToSqlArgs(columns, rv)
	if argsErr != nil {
		err = errors.ServiceError("mysql: insert or update failed, try to fill args failed").WithCause(argsErr).WithMeta("mysql", "insert or update")
		return
	}
	affected, lastInsertId, execErr := sql.Execute(ctx, query, args...)
	if execErr != nil {
		err = errors.ServiceError("mysql: insert or update failed").WithCause(execErr).WithMeta("mysql", "insert or update")
		return
	}
	if affected == 0 {
		return
	}
	// incrPk
	if lastInsertId > 0 {
		pks := tab.findPk()
		for _, pk := range pks {
			if pk.isIncrPk() {
				rv.Elem().FieldByName(pk.FieldName).SetInt(lastInsertId)
				break
			}
		}
	}
	// version
	tryFillAuditVersion(rv, tab)
	return
}

func InsertWhenExist(ctx context.Context, row interface{}, source Select) (err errors.CodeError) {
	execErr := insertWhenExistOrNot(ctx, row, true, source)
	if execErr != nil {
		err = errors.ServiceError("mysql: insert when exist failed").WithCause(execErr).WithMeta("mysql", "insert when exist")
		return
	}
	return
}

func InsertWhenNotExist(ctx context.Context, row interface{}, source Select) (err errors.CodeError) {
	execErr := insertWhenExistOrNot(ctx, row, false, source)
	if execErr != nil {
		err = errors.ServiceError("mysql: insert when not exist failed").WithCause(execErr).WithMeta("mysql", "insert when not exist")
		return
	}
	return
}

func insertWhenExistOrNot(ctx context.Context, row interface{}, exist bool, source Select) (err error) {
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
	args, argsErr := mapColumnsToSqlArgs(columns, rv)
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
	affected, lastInsertId, execErr := sql.Execute(ctx, query, args...)
	if execErr != nil {
		err = execErr
		return
	}
	if affected == 0 {
		return
	}
	// incrPk
	if lastInsertId > 0 {
		pks := tab.findPk()
		for _, pk := range pks {
			if pk.isIncrPk() {
				rv.Elem().FieldByName(pk.FieldName).SetInt(lastInsertId)
				break
			}
		}
	}
	// version
	tryFillAuditVersionExact(rv, tab, int64(1))
	return
}
