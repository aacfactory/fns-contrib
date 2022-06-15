package postgres

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
		err = errors.ServiceError("postgres: insert failed for row is nil").WithMeta("postgres", "insert")
		return
	}
	rv := reflect.ValueOf(row)
	if rv.Type().Kind() != reflect.Ptr {
		err = errors.ServiceError("postgres: insert failed for type of row is not ptr").WithMeta("postgres", "insert")
		return
	}
	if rv.Elem().Type().Kind() != reflect.Struct {
		err = errors.ServiceError("postgres: insert failed for type of row is not ptr struct").WithMeta("postgres", "insert")
		return
	}
	tab := createOrLoadTable(row)
	// create by
	tryFillCreateErr := tryFillAuditCreate(ctx, rv, tab)
	if tryFillCreateErr != nil {
		err = errors.ServiceError("postgres: insert failed, try to fill create audit failed").WithCause(tryFillCreateErr).WithMeta("postgres", "insert")
		return
	}
	// exec
	useQuery := tab.insertQuery.useQuery
	query := tab.insertQuery.query
	columns := tab.insertQuery.columns
	args, argsErr := mapColumnsToSqlArgs(columns, rv)
	if argsErr != nil {
		err = errors.ServiceError("postgres: insert failed, try to fill args failed").WithCause(argsErr).WithMeta("postgres", "insert")
		return
	}
	if useQuery {
		rows, queryErr := sql.Query(ctx, query, args...)
		if queryErr != nil {
			err = errors.ServiceError("postgres: insert failed").WithCause(queryErr).WithMeta("postgres", "insert")
			return
		}
		if rows.Empty() {
			return
		}
		row0, _ := rows.Next()
		lastInsertId := int64(0)
		hasColumn, columnErr := row0.Column("LAST_INSERT_ID", &lastInsertId)
		if columnErr != nil {
			err = errors.ServiceError("postgres: insert failed").WithCause(columnErr).WithMeta("postgres", "insert")
			return
		}
		if !hasColumn {
			err = errors.ServiceError("postgres: insert failed").WithCause(fmt.Errorf("LAST_INSERT_ID is not found in results")).WithMeta("postgres", "insert")
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
	} else {
		affected, _, execErr := sql.Execute(ctx, query, args...)
		if execErr != nil {
			err = errors.ServiceError("postgres: insert failed").WithCause(execErr).WithMeta("postgres", "insert")
			return
		}
		if affected == 0 {
			return
		}
	}
	// version
	tryFillAuditVersionExact(rv, tab, int64(1))
	return
}

func InsertOrUpdate(ctx context.Context, row interface{}) (err errors.CodeError) {
	if row == nil {
		err = errors.ServiceError("postgres: insert or update failed for row is nil").WithMeta("postgres", "insert or update")
		return
	}
	rv := reflect.ValueOf(row)
	if rv.Type().Kind() != reflect.Ptr {
		err = errors.ServiceError("postgres: insert or update failed for type of row is not ptr").WithMeta("postgres", "insert or update")
		return
	}
	if rv.Elem().Type().Kind() != reflect.Struct {
		err = errors.ServiceError("postgres: insert or update failed for type of row is not ptr struct").WithMeta("postgres", "insert or update")
		return
	}
	tab := createOrLoadTable(row)
	querySetting := tab.insertOrUpdateQuery
	if querySetting == nil {
		err = errors.ServiceError("postgres: insert or update failed for type of row is not supported, need conflict or string typed pk").WithMeta("postgres", "insert or update")
		return
	}
	// create
	tryFillCreateErr := tryFillAuditCreate(ctx, rv, tab)
	if tryFillCreateErr != nil {
		err = errors.ServiceError("postgres: insert or update failed, try to fill create audit failed").WithCause(tryFillCreateErr).WithMeta("postgres", "insert or update")
		return
	}
	// modify
	tryFillModifyErr := tryFillAuditModify(ctx, rv, tab)
	if tryFillModifyErr != nil {
		err = errors.ServiceError("postgres: insert or update failed, try to fill modify audit failed").WithCause(tryFillModifyErr).WithMeta("postgres", "insert or update")
		return
	}
	// exec
	useQuery := querySetting.useQuery
	query := querySetting.query
	columns := querySetting.columns
	args, argsErr := mapColumnsToSqlArgs(columns, rv)
	if argsErr != nil {
		err = errors.ServiceError("postgres: insert or update failed, try to fill args failed").WithCause(argsErr).WithMeta("postgres", "insert or update")
		return
	}
	if useQuery {
		rows, queryErr := sql.Query(ctx, query, args...)
		if queryErr != nil {
			err = errors.ServiceError("postgres: insert or update failed").WithCause(queryErr).WithMeta("postgres", "insert or update")
			return
		}
		if rows.Empty() {
			return
		}
		row0, _ := rows.Next()
		lastInsertId := int64(0)
		hasColumn, columnErr := row0.Column("LAST_INSERT_ID", &lastInsertId)
		if columnErr != nil {
			err = errors.ServiceError("postgres: insert or update failed").WithCause(columnErr).WithMeta("postgres", "insert or update")
			return
		}
		if !hasColumn {
			err = errors.ServiceError("postgres: insert or update failed").WithCause(fmt.Errorf("LAST_INSERT_ID is not found in results")).WithMeta("postgres", "insert or update")
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
	} else {
		affected, _, execErr := sql.Execute(ctx, query, args...)
		if execErr != nil {
			err = errors.ServiceError("postgres: insert or update failed").WithCause(execErr).WithMeta("postgres", "insert or update")
			return
		}
		if affected == 0 {
			return
		}
	}
	// version
	tryFillAuditVersion(rv, tab)
	return
}

func InsertWhenExist(ctx context.Context, row interface{}, source Select) (err errors.CodeError) {
	execErr := insertWhenExistOrNot(ctx, row, true, source)
	if execErr != nil {
		err = errors.ServiceError("postgres: insert when exist failed").WithCause(execErr).WithMeta("postgres", "insert when exist")
		return
	}
	return
}

func InsertWhenNotExist(ctx context.Context, row interface{}, source Select) (err errors.CodeError) {
	execErr := insertWhenExistOrNot(ctx, row, false, source)
	if execErr != nil {
		err = errors.ServiceError("postgres: insert when not exist failed").WithCause(execErr).WithMeta("postgres", "insert when not exist")
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
	useQuery := tab.insertWhenExistOrNotQuery.useQuery
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
	if useQuery {
		rows, queryErr := sql.Query(ctx, query, args...)
		if queryErr != nil {
			err = queryErr
			return
		}
		if rows.Empty() {
			return
		}
		row0, _ := rows.Next()
		lastInsertId := int64(0)
		hasColumn, columnErr := row0.Column("LAST_INSERT_ID", &lastInsertId)
		if columnErr != nil {
			err = fmt.Errorf("scan LAST_INSERT_ID failed, %v", columnErr)
			return
		}
		if !hasColumn {
			err = fmt.Errorf("scan LAST_INSERT_ID failed, not found in results")
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
	} else {
		affected, _, execErr := sql.Execute(ctx, query, args...)
		if execErr != nil {
			err = execErr
			return
		}
		if affected == 0 {
			return
		}
	}
	// version
	tryFillAuditVersionExact(rv, tab, int64(1))
	return
}
