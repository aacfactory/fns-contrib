package types

import (
	stdsql "database/sql"
	"encoding/binary"
	"github.com/aacfactory/errors"
	"github.com/aacfactory/fns-contrib/databases/sql"
	"reflect"
)

func IntValueType() sql.ValueType {
	return &intValueType{
		typ:           reflect.TypeOf(int64(1)),
		ct:            "int64",
		databaseTypes: []string{"INT", "SERIAL", "BIGINT", "INTEGER", "SMALLINT", "INT"},
	}
}

type intValueType struct {
	typ           reflect.Type
	ct            string
	databaseTypes []string
}

func (vt *intValueType) Type() (typ reflect.Type) {
	typ = vt.typ
	return
}

func (vt *intValueType) ColumnType() (ct string) {
	ct = vt.ct
	return
}

func (vt *intValueType) DatabaseTypes() (types []string) {
	types = vt.databaseTypes
	return
}

func (vt *intValueType) Scanner() (scanner sql.ValueScanner) {
	scanner = &intValueTypeScanner{
		value: &stdsql.NullInt64{},
	}
	return
}

func (vt *intValueType) Encode(src any) (p []byte, err error) {
	s, ok := src.(int64)
	if !ok {
		err = errors.Warning("sql: int value type encode failed").WithCause(errors.Warning("sql: src is not int64"))
		return
	}
	p = make([]byte, 10)
	binary.PutVarint(p, s)
	return
}

func (vt *intValueType) Decode(p []byte) (v any, err error) {
	n := 0
	v, n = binary.Varint(p)
	if n == 0 {
		err = errors.Warning("sql: decode int value type failed").WithCause(errors.Warning("sql: bytes is too small"))
		return
	}
	if n < 0 {
		err = errors.Warning("sql: decode int value type failed").WithCause(errors.Warning("sql: over 64 bit"))
		return
	}
	return
}

type intValueTypeScanner struct {
	value *stdsql.NullInt64
}

func (vts *intValueTypeScanner) Scan(src any) error {
	return vts.value.Scan(src)
}

func (vts *intValueTypeScanner) Value() (value any) {
	if vts.value.Valid {
		value = vts.value.Int64
		return
	}
	value = int64(0)
	return
}

func (vts *intValueTypeScanner) Reset() {
	vts.value.Valid = false
	vts.value.Int64 = int64(0)
}
