package specifications

import (
	"database/sql"
	"github.com/aacfactory/fns/commons/times"
	"github.com/aacfactory/json"
	"reflect"
	"sort"
	"time"
)

var (
	stringType            = reflect.TypeOf("")
	boolType              = reflect.TypeOf(false)
	intType               = reflect.TypeOf(int64(0))
	floatType             = reflect.TypeOf(float64(0))
	uintType              = reflect.TypeOf(uint64(0))
	datetimeType          = reflect.TypeOf(time.Time{})
	dateType              = reflect.TypeOf(times.Date{})
	timeType              = reflect.TypeOf(times.Time{})
	bytesType             = reflect.TypeOf([]byte{})
	byteType              = reflect.TypeOf(byte(0))
	jsonDateType          = reflect.TypeOf(json.Date{})
	jsonTimeType          = reflect.TypeOf(json.Time{})
	rawType               = reflect.TypeOf(sql.RawBytes{})
	nullStringType        = reflect.TypeOf(sql.NullString{})
	nullBoolType          = reflect.TypeOf(sql.NullBool{})
	nullInt16Type         = reflect.TypeOf(sql.NullInt16{})
	nullInt32Type         = reflect.TypeOf(sql.NullInt32{})
	nullInt64Type         = reflect.TypeOf(sql.NullInt64{})
	nullFloatType         = reflect.TypeOf(sql.NullFloat64{})
	nullByteType          = reflect.TypeOf(sql.NullByte{})
	nullTimeType          = reflect.TypeOf(sql.NullTime{})
	jsonMarshalerType     = reflect.TypeOf((*json.Marshaler)(nil)).Elem()
	jsonUnmarshalerType   = reflect.TypeOf((*json.Unmarshaler)(nil)).Elem()
	scannerType           = reflect.TypeOf((*sql.Scanner)(nil)).Elem()
	anyType               = reflect.TypeOf(new(any)).Elem()
	sortType              = reflect.TypeOf((*sort.Interface)(nil)).Elem()
	tableType             = reflect.TypeOf((*Table)(nil)).Elem()
	queryInterceptorType  = reflect.TypeOf((*QueryInterceptor)(nil)).Elem()
	queryHookType         = reflect.TypeOf((*QueryHook)(nil)).Elem()
	insertInterceptorType = reflect.TypeOf((*InsertInterceptor)(nil)).Elem()
	insertHookType        = reflect.TypeOf((*InsertHook)(nil)).Elem()
	updateInterceptorType = reflect.TypeOf((*UpdateInterceptor)(nil)).Elem()
	updateHookType        = reflect.TypeOf((*UpdateHook)(nil)).Elem()
	deleteInterceptorType = reflect.TypeOf((*DeleteInterceptor)(nil)).Elem()
	deleteHookType        = reflect.TypeOf((*DeleteHook)(nil)).Elem()
)
