package sql

import (
	"database/sql"
	"github.com/aacfactory/fns/commons/times"
	"github.com/aacfactory/json"
	"reflect"
	"time"
)

var (
	stringType   = reflect.TypeOf("")
	boolType     = reflect.TypeOf(false)
	intType      = reflect.TypeOf(int64(0))
	floatType    = reflect.TypeOf(float64(0))
	uintType     = reflect.TypeOf(uint64(0))
	datetimeType = reflect.TypeOf(time.Time{})
	dateType     = reflect.TypeOf(times.Date{})
	timeType     = reflect.TypeOf(times.Time{})
	bytesType    = reflect.TypeOf([]byte{})
	byteType     = reflect.TypeOf(byte(0))
	jsonDateType = reflect.TypeOf(json.Date{})
	jsonTimeType = reflect.TypeOf(json.Time{})
	anyType      = reflect.TypeOf(new(any)).Elem()
	rawType      = reflect.TypeOf(sql.RawBytes{})
)

var (
	nullBytes  = []byte("null")
	trueBytes  = []byte("true")
	falseBytes = []byte("false")
)
