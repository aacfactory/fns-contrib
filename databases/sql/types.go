package sql

import (
	"database/sql"
	"fmt"
	"github.com/aacfactory/errors"
	"github.com/aacfactory/fns/commons/times"
	"github.com/aacfactory/json"
	"reflect"
	"time"
)

var (
	stringType        = reflect.TypeOf("")
	boolType          = reflect.TypeOf(false)
	intType           = reflect.TypeOf(int64(0))
	floatType         = reflect.TypeOf(float64(0))
	uintType          = reflect.TypeOf(uint64(0))
	datetimeType      = reflect.TypeOf(time.Time{})
	dateType          = reflect.TypeOf(times.Date{})
	timeType          = reflect.TypeOf(times.Time{})
	bytesType         = reflect.TypeOf([]byte{})
	byteType          = reflect.TypeOf(byte(0))
	jsonDateType      = reflect.TypeOf(json.Date{})
	jsonTimeType      = reflect.TypeOf(json.Time{})
	anyType           = reflect.TypeOf(new(any)).Elem()
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
	nullBytesType     = reflect.TypeOf(NullBytes{})
)

var (
	nullBytes  = []byte("null")
	trueBytes  = []byte("true")
	falseBytes = []byte("false")
)

type NullJson[E any] struct {
	Valid bool
	Value E
}

func (n *NullJson[E]) UnmarshalJSON(p []byte) error {
	if len(p) == 0 {
		n.Valid = false
		return nil
	}
	if reflect.TypeOf(n.Value).Kind() == reflect.Ptr {
		err := json.Unmarshal(p, n.Value)
		if err != nil {
			return err
		}
	} else {
		err := json.Unmarshal(p, &n.Value)
		if err != nil {
			return err
		}
	}
	n.Valid = true
	return nil
}

func (n NullJson[E]) MarshalJSON() ([]byte, error) {
	if n.Valid {
		return json.Marshal(n.Value)
	}
	return nil, nil
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
