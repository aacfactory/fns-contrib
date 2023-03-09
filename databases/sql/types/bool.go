package types

import (
	stdsql "database/sql"
	"github.com/aacfactory/errors"
	"github.com/aacfactory/fns-contrib/databases/sql"
	"reflect"
)

func BoolValueType() sql.ValueType {
	return &boolValueType{
		typ:           reflect.TypeOf(true),
		ct:            "bool",
		databaseTypes: []string{"BOOL"},
	}
}

type boolValueType struct {
	typ           reflect.Type
	ct            string
	databaseTypes []string
}

func (vt *boolValueType) Type() (typ reflect.Type) {
	typ = vt.typ
	return
}

func (vt *boolValueType) ColumnType() (ct string) {
	ct = vt.ct
	return
}

func (vt *boolValueType) DatabaseTypes() (types []string) {
	types = vt.databaseTypes
	return
}

func (vt *boolValueType) Scanner() (scanner sql.ValueScanner) {
	scanner = &boolValueTypeScanner{
		value: &stdsql.NullBool{},
	}
	return
}

func (vt *boolValueType) Encode(src any) (p []byte, err error) {
	s, ok := src.(bool)
	if !ok {
		err = errors.Warning("sql: bool value type encode failed").WithCause(errors.Warning("sql: src is not bool"))
		return
	}
	if s {
		p = []byte{'1'}
	} else {
		p = []byte{'0'}
	}
	return
}

func (vt *boolValueType) Decode(p []byte) (v any, err error) {
	if len(p) != 1 {
		err = errors.Warning("sql: decode bool value type failed").WithCause(errors.Warning("sql: bytes len is not one"))
		return
	}
	v = p[0] == '1'
	return
}

type boolValueTypeScanner struct {
	value *stdsql.NullBool
}

func (vts *boolValueTypeScanner) Scan(src any) error {
	return vts.value.Scan(src)
}

func (vts *boolValueTypeScanner) Value() (value any) {
	if vts.value.Valid {
		value = vts.value.Bool
		return
	}
	value = false
	return
}

func (vts *boolValueTypeScanner) Reset() {
	vts.value.Valid = false
	vts.value.Bool = false
}
