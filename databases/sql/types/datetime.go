package types

import (
	stdsql "database/sql"
	"github.com/aacfactory/errors"
	"github.com/aacfactory/fns-contrib/databases/sql"
	"github.com/aacfactory/fns/commons/bytex"
	"reflect"
	"time"
)

func DatetimeValueType() sql.ValueType {
	return &datetimeValueType{
		typ:           reflect.TypeOf(time.Time{}),
		ct:            "datetime",
		databaseTypes: []string{"TIMESTAMP"},
	}
}

type datetimeValueType struct {
	typ           reflect.Type
	ct            string
	databaseTypes []string
}

func (vt *datetimeValueType) Type() (typ reflect.Type) {
	typ = vt.typ
	return
}

func (vt *datetimeValueType) ColumnType() (ct string) {
	ct = vt.ct
	return
}

func (vt *datetimeValueType) DatabaseTypes() (types []string) {
	types = vt.databaseTypes
	return
}

func (vt *datetimeValueType) Scanner() (scanner sql.ValueScanner) {
	scanner = &datetimeValueTypeScanner{
		value: &stdsql.NullTime{},
	}
	return
}

func (vt *datetimeValueType) Encode(src any) (p []byte, err error) {
	s, ok := src.(time.Time)
	if !ok {
		err = errors.Warning("sql: datetime value type encode failed").WithCause(errors.Warning("sql: src is not time.Time"))
		return
	}
	p = bytex.FromString(s.Format(time.RFC3339Nano))
	return
}

func (vt *datetimeValueType) Decode(p []byte) (v any, err error) {
	t, parseErr := time.Parse(time.RFC3339Nano, bytex.ToString(p))
	if parseErr != nil {
		err = parseErr
		return
	}
	v = t
	return
}

type datetimeValueTypeScanner struct {
	value *stdsql.NullTime
}

func (vts *datetimeValueTypeScanner) Scan(src any) error {
	return vts.value.Scan(src)
}

func (vts *datetimeValueTypeScanner) Value() (value any) {
	if vts.value.Valid {
		value = vts.value.Time
		return
	}
	value = time.Time{}
	return
}

func (vts *datetimeValueTypeScanner) Reset() {
	vts.value.Valid = false
	vts.value.Time = time.Time{}
}
