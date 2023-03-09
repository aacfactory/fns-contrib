package types

import (
	"github.com/aacfactory/errors"
	"github.com/aacfactory/fns-contrib/databases/sql"
	"github.com/aacfactory/json"
	"reflect"
)

func JsonValueType() sql.ValueType {
	return &jsonValueType{
		typ:           reflect.TypeOf(json.RawMessage{}),
		ct:            "json",
		databaseTypes: []string{"JSON", "JSONB"},
	}
}

type jsonValueType struct {
	typ           reflect.Type
	ct            string
	databaseTypes []string
}

func (vt *jsonValueType) Type() (typ reflect.Type) {
	typ = vt.typ
	return
}

func (vt *jsonValueType) ColumnType() (ct string) {
	ct = vt.ct
	return
}

func (vt *jsonValueType) DatabaseTypes() (types []string) {
	types = vt.databaseTypes
	return
}

func (vt *jsonValueType) Scanner() (scanner sql.ValueScanner) {
	scanner = &jsonValueTypeScanner{
		value: &sql.NullJson{},
	}
	return
}

func (vt *jsonValueType) Encode(src any) (p []byte, err error) {
	s, ok := src.(json.RawMessage)
	if !ok {
		err = errors.Warning("sql: json value type encode failed").WithCause(errors.Warning("sql: src is not json.RawMessage"))
		return
	}
	p = s
	return
}

func (vt *jsonValueType) Decode(p []byte) (v any, err error) {
	v = p
	return
}

type jsonValueTypeScanner struct {
	value *sql.NullJson
}

func (vts *jsonValueTypeScanner) Scan(src any) error {
	return vts.value.Scan(src)
}

func (vts *jsonValueTypeScanner) Value() (value any) {
	if vts.value.Valid {
		value = vts.value.Json
		return
	}
	value = json.RawMessage{}
	return
}

func (vts *jsonValueTypeScanner) Reset() {
	vts.value.Valid = false
	vts.value.Json = json.RawMessage{}
}
