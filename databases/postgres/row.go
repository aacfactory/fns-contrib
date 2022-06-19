package postgres

import (
	"context"
	"github.com/aacfactory/errors"
	"github.com/aacfactory/fns-contrib/databases/sql"
	"reflect"
)

type Row struct {
	value sql.Row
}

func (row *Row) Empty() (ok bool) {
	ok = row.value.Empty()
	return
}

func (row *Row) Column(name string, value interface{}) (has bool, err error) {
	has, err = row.value.Column(name, value)
	return
}

func (row *Row) Scan(ctx context.Context, v interface{}) (err error) {
	err = scanQueryResult(ctx, row.value, reflect.ValueOf(v))
	if err != nil {
		err = errors.ServiceError("postgres: row scan failed").WithCause(err)
		return
	}
	return
}

type Rows struct {
	value sql.Rows
}

func (rows *Rows) Empty() (ok bool) {
	ok = rows.value.Empty()
	return
}

func (rows *Rows) Size() int {
	return rows.value.Size()
}

func (rows *Rows) Next() (v *Row, has bool) {
	row, next := rows.value.Next()
	if !next {
		return
	}
	v = &Row{
		value: row,
	}
	has = true
	return
}

func (rows *Rows) Scan(ctx context.Context, v interface{}) (err error) {
	err = scanQueryResults(ctx, rows.value, reflect.ValueOf(v))
	if err != nil {
		err = errors.ServiceError("postgres: rows scan failed").WithCause(err)
		return
	}
	return
}
