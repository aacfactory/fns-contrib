package specifications

import (
	"database/sql"
	"github.com/aacfactory/fns/commons/times"
	"github.com/aacfactory/json"
	"reflect"
	"time"
	"unsafe"
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
	nullTimeType      = reflect.TypeOf(sql.NullTime{})
	jsonMarshalerType = reflect.TypeOf((*json.Marshaler)(nil)).Elem()
	scannerType       = reflect.TypeOf((*sql.Scanner)(nil)).Elem()
)

func Instance[T any]() (v T) {
	return
}

type FieldName []byte

func (name FieldName) String() string {
	return unsafe.String(unsafe.SliceData(name), len(name))
}
