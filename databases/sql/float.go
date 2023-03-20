package sql

import (
	stdsql "database/sql"
	"github.com/aacfactory/errors"
	"github.com/aacfactory/fns/commons/bytex"
	"reflect"
	"strconv"
)

func FloatValueType() ValueType {
	return &floatValueType{
		typ:           reflect.TypeOf(float64(1)),
		ct:            "float64",
		databaseTypes: []string{"FLOAT", "DOUBLE", "NUMERIC"},
	}
}

type floatValueType struct {
	typ           reflect.Type
	ct            string
	databaseTypes []string
}

func (vt *floatValueType) Type() (typ reflect.Type) {
	typ = vt.typ
	return
}

func (vt *floatValueType) ColumnType() (ct string) {
	ct = vt.ct
	return
}

func (vt *floatValueType) DatabaseTypes() (types []string) {
	types = vt.databaseTypes
	return
}

func (vt *floatValueType) Scanner() (scanner ValueScanner) {
	scanner = &floatValueTypeScanner{
		value: &stdsql.NullFloat64{},
	}
	return
}

func (vt *floatValueType) Encode(src any) (p []byte, err error) {
	s, ok := src.(float64)
	if !ok {
		err = errors.Warning("sql: float value type encode failed").WithCause(errors.Warning("sql: src is not float64"))
		return
	}
	p = bytex.FromString(strconv.FormatFloat(s, 'E', -1, 64))
	return
}

func (vt *floatValueType) Decode(p []byte) (v any, err error) {
	v, err = strconv.ParseFloat(bytex.ToString(p), 64)
	if err != nil {
		err = errors.Warning("sql: decode float value type failed").WithCause(err).WithMeta("value", bytex.ToString(p))
		return
	}
	return
}

type floatValueTypeScanner struct {
	value *stdsql.NullFloat64
}

func (vts *floatValueTypeScanner) Scan(src any) error {
	return vts.value.Scan(src)
}

func (vts *floatValueTypeScanner) Value() (value any) {
	if vts.value.Valid {
		value = vts.value.Float64
		return
	}
	const (
		zero = float64(0.0)
	)
	value = zero
	return
}

func (vts *floatValueTypeScanner) Reset() {
	vts.value.Valid = false
	vts.value.Float64 = 0.0
}
