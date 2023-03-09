package sql

import (
	"github.com/aacfactory/errors"
	"github.com/aacfactory/fns-contrib/databases/sql/internal"
)

type Column interface {
	Type() (typ string)
	DatabaseType() (dbt string)
	Name() (v string)
	IsNil() (ok bool)
	Get(v interface{}) (err error)
	Value() (value any, err error)
}

type column struct {
	Type_         string `json:"type"`
	DatabaseType_ string `json:"databaseType"`
	Name_         string `json:"name"`
	Value_        []byte `json:"value"`
	Nil           bool   `json:"nil"`
}

func (c *column) Type() (typ string) {
	typ = c.Type_
	return
}

func (c *column) DatabaseType() (dbt string) {
	dbt = c.DatabaseType_
	return
}

func (c *column) Name() (v string) {
	v = c.Name_
	return
}

func (c *column) IsNil() (ok bool) {
	ok = c.Nil
	return
}

func (c *column) Get(v interface{}) (err error) {
	if c.Nil {
		return
	}
	cv, getErr := c.Value()
	if getErr != nil {
		err = getErr
		return
	}
	cpErr := internal.CopyInterface(v, cv)
	if cpErr != nil {
		err = errors.Warning("sql: get column value failed").WithCause(cpErr).WithMeta("columnType", c.Type_).WithMeta("databaseType", c.DatabaseType_)
		return
	}
	return
}

func (c *column) Value() (value any, err error) {
	if c.Nil {
		return
	}
	vt, hasVT := findValueTypeByDatabaseType(c.DatabaseType_)
	if !hasVT {
		err = errors.Warning("sql: get column value failed").WithCause(errors.Warning("sql: value type was not registered")).WithMeta("columnType", c.Type_).WithMeta("databaseType", c.DatabaseType_)
		return
	}
	cv, decodeErr := vt.Decode(c.Value_)
	if decodeErr != nil {
		err = errors.Warning("sql: get column value failed").WithCause(decodeErr).WithMeta("columnType", c.Type_).WithMeta("databaseType", c.DatabaseType_)
		return
	}
	value = cv
	return
}
