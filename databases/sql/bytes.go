package sql

import (
	stdsql "database/sql"
	"fmt"
	"github.com/aacfactory/errors"
	"reflect"
)

type NullRawBytes struct {
	Raw   stdsql.RawBytes
	Valid bool
}

func (v *NullRawBytes) Scan(src interface{}) error {
	if src == nil {
		return nil
	}
	switch src.(type) {
	case string:
		x := src.(string)
		if x == "" {
			return nil
		}
		v.Raw = []byte(x)
		v.Valid = true
	case []byte:
		x := src.([]byte)
		if len(x) > 0 {
			v.Raw = x
			v.Valid = true
		}
	default:
		return fmt.Errorf("scan sql raw value failed for %v is not supported", reflect.TypeOf(src).String())
	}

	return nil
}

func BytesValueType() ValueType {
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

func (vt *bytesValueType) Scanner() (scanner ValueScanner) {
	scanner = &bytesValueTypeScanner{
		value: &NullRawBytes{},
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
	value *NullRawBytes
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
