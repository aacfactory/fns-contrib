package databases

import (
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
)
