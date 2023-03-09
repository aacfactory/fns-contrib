package types

import (
	"github.com/aacfactory/errors"
	"github.com/aacfactory/fns-contrib/databases/sql"
	"github.com/aacfactory/fns/commons/bytex"
	"reflect"
	"time"
)

func TimeValueType() sql.ValueType {
	return &timeValueType{
		typ:           reflect.TypeOf(sql.Time{}),
		ct:            "time",
		databaseTypes: []string{"TIME"},
	}
}

type timeValueType struct {
	typ           reflect.Type
	ct            string
	databaseTypes []string
}

func (vt *timeValueType) Type() (typ reflect.Type) {
	typ = vt.typ
	return
}

func (vt *timeValueType) ColumnType() (ct string) {
	ct = vt.ct
	return
}

func (vt *timeValueType) DatabaseTypes() (types []string) {
	types = vt.databaseTypes
	return
}

func (vt *timeValueType) Scanner() (scanner sql.ValueScanner) {
	scanner = &timeValueTypeScanner{
		value: &sql.NullTime{},
	}
	return
}

func (vt *timeValueType) Encode(src any) (p []byte, err error) {
	s, ok := src.(sql.Time)
	if !ok {
		err = errors.Warning("sql: time value type encode failed").WithCause(errors.Warning("sql: src is not sql.Time"))
		return
	}
	p = bytex.FromString(s.String())
	return
}

func (vt *timeValueType) Decode(p []byte) (v any, err error) {
	t, parseErr := time.Parse("15:04:05", bytex.ToString(p))
	if parseErr != nil {
		err = parseErr
		return
	}
	v = sql.NewTimeFromTime(t)
	return
}

type timeValueTypeScanner struct {
	value *sql.NullTime
}

func (vts *timeValueTypeScanner) Scan(src any) error {
	return vts.value.Scan(src)
}

func (vts *timeValueTypeScanner) Value() (value any) {
	if vts.value.Valid {
		value = vts.value.Value
		return
	}
	value = sql.Time{}
	return
}

func (vts *timeValueTypeScanner) Reset() {
	vts.value.Valid = false
	vts.value.Value = sql.Time{}
}
