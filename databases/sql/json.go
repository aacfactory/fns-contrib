package sql

import (
	stdsql "database/sql"
	"github.com/aacfactory/errors"
	"github.com/aacfactory/json"
	"reflect"
)

type NullJson struct {
	Json  json.RawMessage
	Valid bool
}

func (v *NullJson) Scan(src interface{}) error {
	v.Json = []byte("null")
	str := &stdsql.NullString{}
	scanErr := str.Scan(src)
	if scanErr != nil {
		return scanErr
	}
	if str.String == "" {
		return nil
	}
	if json.ValidateString(str.String) {
		v.Valid = true
		v.Json = []byte(str.String)
	}
	return nil
}

func JsonValueType() ValueType {
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

func (vt *jsonValueType) Scanner() (scanner ValueScanner) {
	scanner = &jsonValueTypeScanner{
		value: &NullJson{},
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
	value *NullJson
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
