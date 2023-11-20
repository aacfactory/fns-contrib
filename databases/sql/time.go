package sql

import (
	"github.com/aacfactory/errors"
	"github.com/aacfactory/fns/commons/bytex"
	"github.com/aacfactory/fns/commons/times"
	"reflect"
	"time"
)

type NullTime struct {
	Valid bool
	Value times.Time
}

func (t *NullTime) Scan(src interface{}) error {
	v := &t.Value
	err := v.Scan(src)
	if err != nil {
		return err
	}
	t.Value = *v
	t.Valid = true
	return nil
}

type NullDate struct {
	Valid bool
	Value times.Date
}

func (t *NullDate) Scan(src interface{}) error {
	v := &t.Value
	err := v.Scan(src)
	if err != nil {
		return err
	}
	t.Value = *v
	t.Valid = true
	return nil
}

func TimeValueType() ValueType {
	return &timeValueType{
		typ:           reflect.TypeOf(times.Time{}),
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

func (vt *timeValueType) Scanner() (scanner ValueScanner) {
	scanner = &timeValueTypeScanner{
		value: &NullTime{},
	}
	return
}

func (vt *timeValueType) Encode(src any) (p []byte, err error) {
	s, ok := src.(times.Time)
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
	v = times.MapToTime(t)
	return
}

type timeValueTypeScanner struct {
	value *NullTime
}

func (vts *timeValueTypeScanner) Scan(src any) error {
	return vts.value.Scan(src)
}

func (vts *timeValueTypeScanner) Value() (value any) {
	if vts.value.Valid {
		value = vts.value.Value
		return
	}
	value = times.Time{}
	return
}

func (vts *timeValueTypeScanner) Reset() {
	vts.value.Valid = false
	vts.value.Value = times.Time{}
}
