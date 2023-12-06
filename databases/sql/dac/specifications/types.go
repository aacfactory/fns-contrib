package specifications

import (
	"database/sql"
	ssql "github.com/aacfactory/fns-contrib/databases/sql"
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
	datetimeType      = reflect.TypeOf(time.Time{})
	dateType          = reflect.TypeOf(times.Date{})
	timeType          = reflect.TypeOf(times.Time{})
	bytesType         = reflect.TypeOf([]byte{})
	byteType          = reflect.TypeOf(byte(0))
	rawType           = reflect.TypeOf(sql.RawBytes{})
	nullStringType    = reflect.TypeOf(sql.NullString{})
	nullBoolType      = reflect.TypeOf(sql.NullBool{})
	nullInt16Type     = reflect.TypeOf(sql.NullInt16{})
	nullInt32Type     = reflect.TypeOf(sql.NullInt32{})
	nullInt64Type     = reflect.TypeOf(sql.NullInt64{})
	nullFloatType     = reflect.TypeOf(sql.NullFloat64{})
	nullByteType      = reflect.TypeOf(sql.NullByte{})
	nullBytesType     = reflect.TypeOf(ssql.NullBytes{})
	nullTimeType      = reflect.TypeOf(sql.NullTime{})
	jsonMarshalerType = reflect.TypeOf((*json.Marshaler)(nil)).Elem()
	scannerType       = reflect.TypeOf((*sql.Scanner)(nil)).Elem()
)

var (
	jsonEmptyBytes      = []byte("{}")
	jsonEmptyArrayBytes = []byte("[]")
)

func Instance[T any]() (v T) {
	return
}

func AsInt(e any) (n int64, ok bool) {
	n, ok = e.(int64)
	if ok {
		return
	}
	i32, i32ok := e.(int32)
	if i32ok {
		n = int64(i32)
		ok = true
		return
	}
	i, iok := e.(int)
	if iok {
		n = int64(i)
		ok = true
		return
	}
	i16, i16ok := e.(int16)
	if i16ok {
		n = int64(i16)
		ok = true
		return
	}
	i8, i8ok := e.(int8)
	if i8ok {
		n = int64(i8)
		ok = true
		return
	}
	return
}

func AsFloat(e any) (n float64, ok bool) {
	n, ok = e.(float64)
	if ok {
		return
	}
	f32, f32ok := e.(float32)
	if f32ok {
		n = float64(f32)
		ok = true
		return
	}
	return
}
