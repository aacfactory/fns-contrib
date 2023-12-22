package sql

import (
	"database/sql"
	"database/sql/driver"
	"fmt"
	"github.com/aacfactory/errors"
	"github.com/aacfactory/fns/commons/times"
	"github.com/aacfactory/json"
	"reflect"
	"time"
)

var (
	datetimeType      = reflect.TypeOf(time.Time{})
	dateType          = reflect.TypeOf(times.Date{})
	timeType          = reflect.TypeOf(times.Time{})
	bytesType         = reflect.TypeOf([]byte{})
	byteType          = reflect.TypeOf(byte(0))
	jsonDateType      = reflect.TypeOf(json.Date{})
	jsonTimeType      = reflect.TypeOf(json.Time{})
	rawType           = reflect.TypeOf(sql.RawBytes{})
	jsonMarshalerType = reflect.TypeOf((*json.Marshaler)(nil)).Elem()
	nullStringType    = reflect.TypeOf(sql.NullString{})
	nullBoolType      = reflect.TypeOf(sql.NullBool{})
	nullInt16Type     = reflect.TypeOf(sql.NullInt16{})
	nullInt32Type     = reflect.TypeOf(sql.NullInt32{})
	nullInt64Type     = reflect.TypeOf(sql.NullInt64{})
	nullFloatType     = reflect.TypeOf(sql.NullFloat64{})
	nullByteType      = reflect.TypeOf(sql.NullByte{})
	nullTimeType      = reflect.TypeOf(sql.NullTime{})
)

var (
	nullBytes = []byte("null")
)

type Scanner interface {
	sql.Scanner
	driver.Valuer
	json.Marshaler
	json.Unmarshaler
}

func NewNullString(s string) NullString {
	return NullString{
		sql.NullString{
			String: s,
			Valid:  s != "",
		},
	}
}

type NullString struct {
	sql.NullString
}

func (n NullString) MarshalJSON() (p []byte, err error) {
	if n.Valid {
		p, err = json.Marshal(n.String)
	}
	return
}

func (n *NullString) UnmarshalJSON(p []byte) error {
	if len(p) == 0 {
		n.Valid = false
		return nil
	}
	err := json.Unmarshal(p, &n.String)
	if err != nil {
		return err
	}
	n.Valid = true
	return nil
}

func NewNullBool(b bool) NullBool {
	return NullBool{
		sql.NullBool{
			Bool:  b,
			Valid: true,
		},
	}
}

type NullBool struct {
	sql.NullBool
}

func (n NullBool) MarshalJSON() (p []byte, err error) {
	if n.Valid {
		p, err = json.Marshal(n.Bool)
	}
	return
}

func (n *NullBool) UnmarshalJSON(p []byte) error {
	if len(p) == 0 {
		n.Valid = false
		return nil
	}
	err := json.Unmarshal(p, &n.Bool)
	if err != nil {
		return err
	}
	n.Valid = true
	return nil
}

func NewNullInt16(n int16) NullInt16 {
	return NullInt16{
		sql.NullInt16{
			Int16: n,
			Valid: true,
		},
	}
}

type NullInt16 struct {
	sql.NullInt16
}

func (n NullInt16) MarshalJSON() (p []byte, err error) {
	if n.Valid {
		p, err = json.Marshal(n.Int16)
	}
	return
}

func (n *NullInt16) UnmarshalJSON(p []byte) error {
	if len(p) == 0 {
		n.Valid = false
		return nil
	}
	err := json.Unmarshal(p, &n.Int16)
	if err != nil {
		return err
	}
	n.Valid = true
	return nil
}

func NewNullInt32(n int32) NullInt32 {
	return NullInt32{
		sql.NullInt32{
			Int32: n,
			Valid: true,
		},
	}
}

type NullInt32 struct {
	sql.NullInt32
}

func (n NullInt32) MarshalJSON() (p []byte, err error) {
	if n.Valid {
		p, err = json.Marshal(n.Int32)
	}
	return
}

func (n *NullInt32) UnmarshalJSON(p []byte) error {
	if len(p) == 0 {
		n.Valid = false
		return nil
	}
	err := json.Unmarshal(p, &n.Int32)
	if err != nil {
		return err
	}
	n.Valid = true
	return nil
}

func NewNullInt64(n int64) NullInt64 {
	return NullInt64{
		sql.NullInt64{
			Int64: n,
			Valid: true,
		},
	}
}

type NullInt64 struct {
	sql.NullInt64
}

func (n NullInt64) MarshalJSON() (p []byte, err error) {
	if n.Valid {
		p, err = json.Marshal(n.Int64)
	}
	return
}

func (n *NullInt64) UnmarshalJSON(p []byte) error {
	if len(p) == 0 {
		n.Valid = false
		return nil
	}
	err := json.Unmarshal(p, &n.Int64)
	if err != nil {
		return err
	}
	n.Valid = true
	return nil
}

func NewNullFloat64(n float64) NullFloat64 {
	return NullFloat64{
		sql.NullFloat64{
			Float64: n,
			Valid:   true,
		},
	}
}

type NullFloat64 struct {
	sql.NullFloat64
}

func (n NullFloat64) MarshalJSON() (p []byte, err error) {
	if n.Valid {
		p, err = json.Marshal(n.Float64)
	}
	return
}

func (n *NullFloat64) UnmarshalJSON(p []byte) error {
	if len(p) == 0 {
		n.Valid = false
		return nil
	}
	err := json.Unmarshal(p, &n.Float64)
	if err != nil {
		return err
	}
	n.Valid = true
	return nil
}

func NewNullByte(b byte) NullByte {
	return NullByte{
		sql.NullByte{
			Byte:  b,
			Valid: true,
		},
	}
}

type NullByte struct {
	sql.NullByte
}

func (n NullByte) MarshalJSON() (p []byte, err error) {
	if n.Valid {
		p, err = json.Marshal(n.Byte)
	}
	return
}

func (n *NullByte) UnmarshalJSON(p []byte) error {
	if len(p) == 0 {
		n.Valid = false
		return nil
	}
	err := json.Unmarshal(p, &n.Byte)
	if err != nil {
		return err
	}
	n.Valid = true
	return nil
}

func NewNullDatetime(t time.Time) NullDatetime {
	return NullDatetime{
		sql.NullTime{
			Time:  t,
			Valid: !t.IsZero(),
		},
	}
}

type NullDatetime struct {
	sql.NullTime
}

func (n NullDatetime) MarshalJSON() (p []byte, err error) {
	if n.Valid {
		p, err = json.Marshal(n.Time)
	}
	return
}

func (n *NullDatetime) UnmarshalJSON(p []byte) error {
	if len(p) == 0 {
		n.Valid = false
		return nil
	}
	err := json.Unmarshal(p, &n.Time)
	if err != nil {
		return err
	}
	n.Valid = true
	return nil
}

func NewNullDate(v times.Date) NullDate {
	return NullDate{
		Valid: !v.IsZero(),
		Date:  v,
	}
}

type NullDate struct {
	Valid bool
	Date  times.Date
}

func (n *NullDate) Scan(src any) error {
	if src == nil {
		return nil
	}
	v := sql.NullTime{}
	if err := v.Scan(src); err != nil {
		return err
	}
	n.Valid = v.Valid
	if n.Valid {
		n.Date = times.DataOf(v.Time)
	}
	return nil
}

func (n NullDate) MarshalJSON() (p []byte, err error) {
	if n.Valid {
		p, err = json.Marshal(n.Date)
	}
	return
}

func (n *NullDate) UnmarshalJSON(p []byte) error {
	if len(p) == 0 {
		n.Valid = false
		return nil
	}
	err := json.Unmarshal(p, &n.Date)
	if err != nil {
		return err
	}
	n.Valid = true
	return nil
}

func (n NullDate) Value() (driver.Value, error) {
	if !n.Valid {
		return nil, nil
	}
	return n.Date.ToTime(), nil
}

func NewNullTime(v times.Time) NullTime {
	return NullTime{
		Valid: !v.IsZero(),
		Time:  v,
	}
}

type NullTime struct {
	Valid bool
	Time  times.Time
}

func (n *NullTime) Scan(src any) error {
	if src == nil {
		return nil
	}
	v := sql.NullTime{}
	if err := v.Scan(src); err != nil {
		return err
	}
	n.Valid = v.Valid
	if n.Valid {
		n.Time = times.TimeOf(v.Time)
	}
	return nil
}

func (n NullTime) MarshalJSON() (p []byte, err error) {
	if n.Valid {
		p, err = json.Marshal(n.Time)
	}
	return
}

func (n *NullTime) UnmarshalJSON(p []byte) error {
	if len(p) == 0 {
		n.Valid = false
		return nil
	}
	err := json.Unmarshal(p, &n.Time)
	if err != nil {
		return err
	}
	n.Valid = true
	return nil
}

func (n NullTime) Value() (driver.Value, error) {
	if !n.Valid {
		return nil, nil
	}
	return n.Time.ToTime(), nil
}

func NewNullJson[E any](e E) NullJson[E] {
	valid := false
	rv := reflect.ValueOf(e)
	switch rv.Type().Kind() {
	case reflect.Struct:
		valid = !rv.IsZero()
		break
	case reflect.Ptr:
		valid = !rv.IsNil()
		break
	case reflect.Slice, reflect.Map:
		valid = rv.Len() > 0
		break
	default:
		break
	}
	return NullJson[E]{
		Valid: valid,
		E:     e,
	}
}

type NullJson[E any] struct {
	Valid bool
	E     E
}

func (n *NullJson[E]) UnmarshalJSON(p []byte) error {
	if len(p) == 0 {
		n.Valid = false
		return nil
	}
	if reflect.TypeOf(n.E).Kind() == reflect.Ptr {
		err := json.Unmarshal(p, n.E)
		if err != nil {
			return err
		}
	} else {
		err := json.Unmarshal(p, &n.E)
		if err != nil {
			return err
		}
	}
	n.Valid = true
	return nil
}

func (n NullJson[E]) MarshalJSON() ([]byte, error) {
	if n.Valid {
		return json.Marshal(n.E)
	}
	return nullBytes, nil
}

func (n *NullJson[E]) Scan(src any) error {
	if src == nil {
		return nil
	}
	p, ok := src.([]byte)
	if !ok {
		return errors.Warning("sql: null json scan failed").WithCause(fmt.Errorf("src is not bytes"))
	}
	err := n.UnmarshalJSON(p)
	if err != nil {
		return errors.Warning("sql: null json scan failed").WithCause(err)
	}
	return nil
}

func (n NullJson[E]) Value() (driver.Value, error) {
	if !n.Valid {
		return nullBytes, nil
	}
	p, encodeErr := json.Marshal(n.E)
	if encodeErr != nil {
		return nil, errors.Warning("sql: null json make driver value failed").WithCause(encodeErr)
	}
	return p, nil
}

func NewNullBytes(p []byte) NullBytes {
	return NullBytes{
		Valid: len(p) > 0,
		Bytes: p,
	}
}

type NullBytes struct {
	Valid bool
	Bytes []byte
}

func (n *NullBytes) Scan(src any) error {
	if src == nil {
		return nil
	}
	switch s := src.(type) {
	case []byte:
		if len(s) > 0 {
			n.Bytes = append(n.Bytes, s...)
			n.Valid = true
		}
		break
	case string:
		if len(s) > 0 {
			n.Bytes = append(n.Bytes, s...)
			n.Valid = true
		}
		break
	default:
		return errors.Warning("sql: null bytes scan failed").WithCause(fmt.Errorf("src is not bytes or string"))
	}
	return nil
}

func (n NullBytes) Value() (driver.Value, error) {
	if !n.Valid {
		return nil, nil
	}
	return n.Bytes, nil
}

func (n NullBytes) MarshalJSON() ([]byte, error) {
	if n.Valid {
		if json.Validate(n.Bytes) {
			return n.Bytes, nil
		}
		return json.Marshal(n.Bytes)
	}
	return nil, nil
}

func (n *NullBytes) UnmarshalJSON(p []byte) error {
	if len(p) == 0 {
		n.Valid = false
		return nil
	}
	if json.Validate(p) {
		n.Bytes = p
		n.Valid = true
		return nil
	}
	err := json.Unmarshal(p, &n.Bytes)
	if err != nil {
		return err
	}
	n.Valid = true
	return nil
}
