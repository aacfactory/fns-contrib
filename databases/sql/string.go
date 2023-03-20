package sql

import (
	stdsql "database/sql"
	"github.com/aacfactory/errors"
	"github.com/aacfactory/fns/commons/bytex"
	"reflect"
)

func StringValueType() ValueType {
	return &stringValueType{
		typ:           reflect.TypeOf(""),
		ct:            "string",
		databaseTypes: []string{"VARCHAR", "CHAR", "TEXT", "CHARACTER VARYING", "CHARACTER"},
	}
}

type stringValueType struct {
	typ           reflect.Type
	ct            string
	databaseTypes []string
}

func (vt *stringValueType) Type() (typ reflect.Type) {
	typ = vt.typ
	return
}

func (vt *stringValueType) ColumnType() (ct string) {
	ct = vt.ct
	return
}

func (vt *stringValueType) DatabaseTypes() (types []string) {
	types = vt.databaseTypes
	return
}

func (vt *stringValueType) Scanner() (scanner ValueScanner) {
	scanner = &stringValueTypeScanner{
		value: &stdsql.NullString{},
	}
	return
}

func (vt *stringValueType) Encode(src any) (p []byte, err error) {
	s, ok := src.(string)
	if !ok {
		err = errors.Warning("sql: string value type encode failed").WithCause(errors.Warning("sql: src is not string"))
		return
	}
	p = bytex.FromString(s)
	return
}

func (vt *stringValueType) Decode(p []byte) (v any, err error) {
	v = bytex.ToString(p)
	return
}

type stringValueTypeScanner struct {
	value *stdsql.NullString
}

func (vts *stringValueTypeScanner) Scan(src any) error {
	return vts.value.Scan(src)
}

func (vts *stringValueTypeScanner) Value() (value any) {
	if vts.value.Valid {
		value = vts.value.String
		return
	}
	value = ""
	return
}

func (vts *stringValueTypeScanner) Reset() {
	vts.value.Valid = false
	vts.value.String = ""
}
