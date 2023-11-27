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

func (n *NullJson[E]) MarshalJSON() ([]byte, error) {
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
