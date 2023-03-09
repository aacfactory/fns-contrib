package types

import (
	"github.com/aacfactory/errors"
	"github.com/aacfactory/fns-contrib/databases/sql"
	"github.com/aacfactory/fns/commons/bytex"
	"reflect"
	"time"
)

func DateValueType() sql.ValueType {
	return &dateValueType{
		typ:           reflect.TypeOf(sql.Date{}),
		ct:            "date",
		databaseTypes: []string{"DATE"},
	}
}

type dateValueType struct {
	typ           reflect.Type
	ct            string
	databaseTypes []string
}

func (vt *dateValueType) Type() (typ reflect.Type) {
	typ = vt.typ
	return
}

func (vt *dateValueType) ColumnType() (ct string) {
	ct = vt.ct
	return
}

func (vt *dateValueType) DatabaseTypes() (types []string) {
	types = vt.databaseTypes
	return
}

func (vt *dateValueType) Scanner() (scanner sql.ValueScanner) {
	scanner = &dateValueTypeScanner{
		value: &sql.NullDate{},
	}
	return
}

func (vt *dateValueType) Encode(src any) (p []byte, err error) {
	s, ok := src.(sql.Date)
	if !ok {
		err = errors.Warning("sql: date value type encode failed").WithCause(errors.Warning("sql: src is not sql.Date"))
		return
	}
	p = bytex.FromString(s.String())
	return
}

func (vt *dateValueType) Decode(p []byte) (v any, err error) {
	t, parseErr := time.Parse("2006-01-02", bytex.ToString(p))
	if parseErr != nil {
		err = parseErr
		return
	}
	v = sql.NewDateFromTime(t)
	return
}

type dateValueTypeScanner struct {
	value *sql.NullDate
}

func (vts *dateValueTypeScanner) Scan(src any) error {
	return vts.value.Scan(src)
}

func (vts *dateValueTypeScanner) Value() (value any) {
	if vts.value.Valid {
		value = vts.value.Value
		return
	}
	value = sql.Date{}
	return
}

func (vts *dateValueTypeScanner) Reset() {
	vts.value.Valid = false
	vts.value.Value = sql.Date{}
}
