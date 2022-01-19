package postgres

import (
	"github.com/aacfactory/errors"
	"github.com/aacfactory/fns"
	"reflect"
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
	//tab := createOrLoadTable(row)
	// create by
	//creates := tab.findAuditCreate()
	return
}

func InsertOrUpdate(ctx fns.Context, row interface{}) (err errors.CodeError) {

	return
}
