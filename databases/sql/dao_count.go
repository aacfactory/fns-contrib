package sql

import (
	"fmt"
	"github.com/aacfactory/errors"
	"github.com/aacfactory/fns"
)

type daoCountRow struct {
	Value int64 `col:"__C"`
}

func (d *dao) Count(ctx fns.Context, param *QueryParam, row TableRow) (num int, err errors.CodeError) {
	if row == nil {
		panic(fmt.Sprintf("fns SQL: use DAO failed for row can not be nil"))
	}
	info := getTableRowInfo(row)
	ns := info.Namespace
	name := info.Name
	alias := info.Alias
	selects := "__C"
	if dialect == "postgres" {
		ns = tableInfoConvertToPostgresName(ns)
		name = tableInfoConvertToPostgresName(name)
		alias = tableInfoConvertToPostgresName(alias)
		selects = tableInfoConvertToPostgresName(selects)
	}
	query := "SELECT COUNT(1) AS " + selects
	params := NewTuple()
	if ns != "" {
		query = query + " FROM " + ns + "." + name + " AS " + alias
	} else {
		query = query + " FROM " + name + " AS " + alias
	}
	if param != nil {
		query = query + " " + param.mapToConditionString(alias, params)
	}
	// do
	results, queryErr := Query(ctx, Param{
		Query: query,
		Args:  params,
	})
	if queryErr != nil {
		err = queryErr
		return
	}
	if results.Empty() {
		return
	}
	v := &daoCountRow{}
	scanErr := results.Scan(v)
	if scanErr != nil {
		err = errors.ServiceError("fns SQL: use DAO failed for scan rows in Query").WithCause(scanErr)
		return
	}
	num = int(v.Value)
	return
}
