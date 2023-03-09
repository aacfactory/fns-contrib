package types

import (
	"github.com/aacfactory/errors"
	"github.com/aacfactory/fns-contrib/databases/sql"
	"reflect"
)

func BytesValueType() sql.ValueType {
	return &bytesValueType{
		typ:           reflect.TypeOf([]byte{}),
		ct:            "bytes",
		databaseTypes: []string{"BLOB"},
	}
}

type bytesValueType struct {
	typ           reflect.Type
	ct            string
	databaseTypes []string
}

func (vt *bytesValueType) Type() (typ reflect.Type) {
	typ = vt.typ
	return
}

func (vt *bytesValueType) ColumnType() (ct string) {
	ct = vt.ct
	return
}

func (vt *bytesValueType) DatabaseTypes() (types []string) {
	types = vt.databaseTypes
	return
}

func (vt *bytesValueType) Scanner() (scanner sql.ValueScanner) {
	scanner = &bytesValueTypeScanner{
		value: &sql.NullRawBytes{},
	}
	return
}

func (vt *bytesValueType) Encode(src any) (p []byte, err error) {
	s, ok := src.([]byte)
	if !ok {
		err = errors.Warning("sql: raw bytes value type encode failed").WithCause(errors.Warning("sql: src is not []byte"))
		return
	}
	p = s
	return
}

func (vt *bytesValueType) Decode(p []byte) (v any, err error) {
	v = p
	return
}

type bytesValueTypeScanner struct {
	value *sql.NullRawBytes
}

func (vts *bytesValueTypeScanner) Scan(src any) error {
	return vts.value.Scan(src)
}

func (vts *bytesValueTypeScanner) Value() (value any) {
	if vts.value.Valid {
		value = vts.value.Raw
		return
	}
	value = []byte{}
	return
}

func (vts *bytesValueTypeScanner) Reset() {
	vts.value.Valid = false
	vts.value.Raw = []byte{}
}
