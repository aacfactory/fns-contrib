package databases

import (
	"database/sql"
	stdJson "encoding/json"
	"fmt"
	"github.com/aacfactory/errors"
	"github.com/aacfactory/fns/commons/bytex"
	"github.com/aacfactory/fns/commons/times"
	"github.com/aacfactory/json"
	"reflect"
	"time"
)

type Arguments []interface{}

func (arguments *Arguments) Len() (n int) {
	n = len(*arguments)
	return
}

func (arguments *Arguments) Append(v interface{}) {
	vv := *arguments
	vv = append(vv, v)
	*arguments = vv
	return
}

func (arguments *Arguments) MarshalJSON() (p []byte, err error) {
	size := arguments.Len()
	if size == 0 {
		p = []byte{'[', ']'}
		return
	}
	vv := make([]Argument, 0, arguments.Len())
	for _, v := range *arguments {
		argument, argumentErr := NewArgument(v)
		if argumentErr != nil {
			err = argumentErr
			return
		}
		vv = append(vv, argument)
	}
	p, err = json.Marshal(vv)
	return
}

func (arguments *Arguments) UnmarshalJSON(p []byte) (err error) {
	vv := make([]Argument, 0, 1)
	err = json.Unmarshal(p, &vv)
	if err != nil {
		return
	}
	ss := *arguments
	for _, argument := range vv {
		v, vErr := argument.Interface()
		if vErr != nil {
			err = vErr
			return
		}
		ss = append(ss, v)
	}
	*arguments = ss
	return
}

func NewArgument(v interface{}) (argument Argument, err error) {
	if v == nil {
		argument.Nil = true
		return
	}
	switch vv := v.(type) {
	case string:
		argument.Type = "string"
		argument.Value = bytex.FromString(fmt.Sprintf("\"%s\"", vv))
		break
	case bool:
		argument.Type = "bool"
		if vv {
			argument.Value = trueBytes
		} else {
			argument.Value = falseBytes
		}
		break
	case int, int8, int16, int32, int64:
		argument.Type = "int"
		argument.Value, _ = json.Marshal(vv)
		break
	case float32, float64:
		argument.Type = "float"
		argument.Value, _ = json.Marshal(vv)
		break
	case uint, uint16, uint32, uint64:
		argument.Type = "uint"
		argument.Value, _ = json.Marshal(vv)
		break
	case time.Time:
		argument.Type = "datetime"
		argument.Value, _ = json.Marshal(vv)
		break
	case times.Date, json.Date:
		argument.Type = "date"
		argument.Value, _ = json.Marshal(vv)
		break
	case times.Time, json.Time:
		argument.Type = "time"
		argument.Value, _ = json.Marshal(vv)
		break
	case json.RawMessage:
		argument.Type = "json"
		argument.Value = vv
		break
	case stdJson.RawMessage:
		argument.Type = "json"
		argument.Value = json.RawMessage(vv)
		break
	case []byte, sql.RawBytes:
		argument.Type = "bytes"
		argument.Value, _ = json.Marshal(vv)
		break
	case byte:
		argument.Type = "byte"
		argument.Value, _ = json.Marshal(vv)
		break
	case sql.NullByte:
		argument.Type = "byte"
		if vv.Valid {
			argument.Value, _ = json.Marshal(vv.Byte)
		} else {
			argument.Nil = true
		}
		break
	case sql.NullBool:
		argument.Type = "bool"
		if vv.Valid {
			if vv.Bool {
				argument.Value = trueBytes
			} else {
				argument.Value = falseBytes
			}
		} else {
			argument.Nil = true
		}
		break
	case sql.NullFloat64:
		argument.Type = "float"
		if vv.Valid {
			argument.Value, _ = json.Marshal(vv.Float64)
		} else {
			argument.Nil = true
		}
		break
	case sql.NullInt16:
		argument.Type = "int"
		if vv.Valid {
			argument.Value, _ = json.Marshal(vv.Int16)
		} else {
			argument.Nil = true
		}
		break
	case sql.NullInt32:
		argument.Type = "int"
		if vv.Valid {
			argument.Value, _ = json.Marshal(vv.Int32)
		} else {
			argument.Nil = true
		}
		break
	case sql.NullInt64:
		argument.Type = "int"
		if vv.Valid {
			argument.Value, _ = json.Marshal(vv.Int64)
		} else {
			argument.Nil = true
		}
		break
	case sql.NullString:
		argument.Type = "int"
		if vv.Valid {
			argument.Value = bytex.FromString(fmt.Sprintf("\"%s\"", vv.String))
		} else {
			argument.Nil = true
		}
		break
	case sql.NullTime:
		argument.Type = "datetime"
		if vv.Valid {
			argument.Value, _ = json.Marshal(vv.Time)
		} else {
			argument.Nil = true
		}
		break
	default:
		rv := reflect.ValueOf(v)
		rt := rv.Type()
		if rt.Kind() == reflect.Pointer {
			err = errors.Warning("sql: new argument failed").WithCause(fmt.Errorf("value must be object value")).WithMeta("type", rt.String())
			return
		}
		if rt.ConvertibleTo(stringType) {
			argument.Type = "string"
			argument.Value = bytex.FromString(fmt.Sprintf("\"%s\"", rv.Convert(stringType).String()))
		} else if rt.ConvertibleTo(boolType) {
			argument.Type = "bool"
			if rv.Convert(boolType).Bool() {
				argument.Value = trueBytes
			} else {
				argument.Value = falseBytes
			}
		} else if rt.ConvertibleTo(intType) {
			argument.Type = "int"
			argument.Value, _ = json.Marshal(rv.Convert(intType).Int())
		} else if rt.ConvertibleTo(floatType) {
			argument.Type = "float"
			argument.Value, _ = json.Marshal(rv.Convert(floatType).Float())
		} else if rt.ConvertibleTo(uintType) {
			argument.Type = "uint"
			argument.Value, _ = json.Marshal(rv.Convert(uintType).Uint())
		} else if rt.ConvertibleTo(datetimeType) {
			argument.Type = "datetime"
			argument.Value, _ = json.Marshal(rv.Convert(datetimeType).Interface())
		} else if rt.ConvertibleTo(dateType) {
			argument.Type = "date"
			argument.Value, _ = json.Marshal(rv.Convert(dateType).Interface())
		} else if rt.ConvertibleTo(timeType) {
			argument.Type = "time"
			argument.Value, _ = json.Marshal(rv.Convert(timeType).Interface())
		} else if rt.ConvertibleTo(jsonDateType) {
			argument.Type = "date"
			argument.Value, _ = json.Marshal(rv.Convert(jsonDateType).Interface())
		} else if rt.ConvertibleTo(jsonTimeType) {
			argument.Type = "time"
			argument.Value, _ = json.Marshal(rv.Convert(jsonTimeType).Interface())
		} else if rt.ConvertibleTo(bytesType) {
			argument.Type = "bytes"
			argument.Value, _ = json.Marshal(rv.Convert(bytesType).Bytes())
		} else if rt.ConvertibleTo(byteType) {
			argument.Type = "byte"
			argument.Value, _ = json.Marshal(rv.Convert(byteType).Interface())
		} else {
			err = errors.Warning("sql: new argument failed").WithCause(fmt.Errorf("type of value is not supported")).WithMeta("type", rt.String())
			return
		}
		break
	}
	return
}

type Argument struct {
	Nil   bool            `json:"nil"`
	Type  string          `json:"type"`
	Value json.RawMessage `json:"value"`
}

func (argument *Argument) Interface() (v interface{}, err error) {
	if argument.Nil {
		return
	}
	switch argument.Type {
	case "string":
		s := ""
		if len(argument.Value) > 0 {
			_ = json.Unmarshal(argument.Value, &s)
		}
		v = s
		break
	case "int":
		i := int64(0)
		if len(argument.Value) > 0 {
			_ = json.Unmarshal(argument.Value, &i)
		}
		v = i
		break
	case "float":
		f := float64(0)
		if len(argument.Value) > 0 {
			_ = json.Unmarshal(argument.Value, &f)
		}
		v = f
		break
	case "uint":
		u := uint64(0)
		if len(argument.Value) > 0 {
			_ = json.Unmarshal(argument.Value, &u)
		}
		v = u
		break
	case "bool":
		b := false
		if len(argument.Value) > 0 {
			_ = json.Unmarshal(argument.Value, &b)
		}
		v = b
		break
	case "json":
		v = argument.Value
		break
	case "byte":
		b := byte(0)
		if len(argument.Value) > 0 {
			_ = json.Unmarshal(argument.Value, &b)
		}
		v = b
	case "bytes":
		// raw
		p := sql.RawBytes{}
		if len(argument.Value) > 0 {
			_ = json.Unmarshal(argument.Value, &p)
		}
		v = p
		break
	case "datetime":
		t := time.Time{}
		if len(argument.Value) > 0 {
			_ = json.Unmarshal(argument.Value, &t)
		}
		v = t
		break
	case "date":
		d := times.Date{}
		if len(argument.Value) > 0 {
			_ = json.Unmarshal(argument.Value, &d)
		}
		v = d
		break
	case "time":
		t := times.Time{}
		if len(argument.Value) > 0 {
			_ = json.Unmarshal(argument.Value, &t)
		}
		v = t
		break
	default:
		err = errors.Warning("sql: unknown argument type").WithMeta("type", argument.Type)
		return
	}
	return
}
