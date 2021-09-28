package sql

import (
	"github.com/aacfactory/errors"
	"github.com/aacfactory/fns"
	"reflect"
)

func (d *dao) Exist(ctx fns.Context, row TableRow) (has bool, err errors.CodeError) {
	info := getTableRowInfo(row)
	query := info.ExistQuery.Query
	paramFields := info.ExistQuery.Params
	rv := reflect.Indirect(reflect.ValueOf(row))
	params := NewTuple()
	for _, field := range paramFields {
		params.Append(rv.FieldByName(field).Interface())
	}
	// do
	rows, queryErr := Query(ctx, Param{
		Query: query,
		Args:  params,
	})
	if queryErr != nil {
		err = queryErr
		return
	}
	if !rows.Empty() {
		has = true
	}
	return
}
