package sql

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

type Arguments []any

func (arguments Arguments) Len() (n int) {
	n = len(arguments)
	return
}

func (arguments Arguments) MarshalJSON() (p []byte, err error) {
	size := arguments.Len()
	if size == 0 {
		p = []byte{'[', ']'}
		return
	}
	vv := make([]Argument, 0, arguments.Len())
	for _, v := range arguments {
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

func NewArgument(v any) (argument Argument, err error) {
	if v == nil {
		argument.Nil = true
		return
	}
	named, isNamed := v.(sql.NamedArg)
	if isNamed {
		argument.Name = named.Name
		v = named.Value
		if v == nil {
			argument.Nil = true
			return
		}
	}
	switch vv := v.(type) {
	case string:
		argument.Type = "string"
		argument.Value = bytex.FromString(fmt.Sprintf("\"%s\"", vv))
		break
	case NullString:
		argument.Type = "string"
		if vv.Valid {
			argument.Value = bytex.FromString(fmt.Sprintf("\"%s\"", vv.String))
		} else {
			argument.Nil = true
		}
		break
	case sql.NullString:
		argument.Type = "string"
		if vv.Valid {
			argument.Value = bytex.FromString(fmt.Sprintf("\"%s\"", vv.String))
		} else {
			argument.Nil = true
		}
		break
	case bool:
		argument.Type = "bool"
		if vv {
			argument.Value = trueBytes
		} else {
			argument.Value = falseBytes
		}
		break
	case NullBool:
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
	case int, int8, int16, int32, int64:
		argument.Type = "int"
		argument.Value, _ = json.Marshal(vv)
		break
	case NullInt64:
		argument.Type = "int"
		if vv.Valid {
			argument.Value, _ = json.Marshal(vv.Int64)
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
	case NullInt32:
		argument.Type = "int"
		if vv.Valid {
			argument.Value, _ = json.Marshal(vv.Int32)
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
	case NullInt16:
		argument.Type = "int"
		if vv.Valid {
			argument.Value, _ = json.Marshal(vv.Int16)
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
	case float32, float64:
		argument.Type = "float"
		argument.Value, _ = json.Marshal(vv)
		break
	case NullFloat64:
		argument.Type = "float"
		if vv.Valid {
			argument.Value, _ = json.Marshal(vv.Float64)
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
	case NullTime:
		argument.Type = "datetime"
		if vv.Valid {
			argument.Value, _ = json.Marshal(vv.Time)
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
	case json.RawMessage:
		argument.Type = "json"
		argument.Value = vv
		break
	case stdJson.RawMessage:
		argument.Type = "json"
		argument.Value = vv
		break
	case []byte, sql.RawBytes:
		argument.Type = "bytes"
		argument.Value, _ = json.Marshal(vv)
		break
	case NullBytes:
		argument.Type = "byte"
		if vv.Valid {
			argument.Value, _ = json.Marshal(vv.Bytes)
		} else {
			argument.Nil = true
		}
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
	default:
		rv := reflect.ValueOf(v)
		rt := rv.Type()
		if rt.Kind() == reflect.Pointer {
			err = errors.Warning("sql: new argument failed").WithCause(fmt.Errorf("value must be object value")).WithMeta("type", rt.String())
			return
		}
		switch rt.Kind() {
		case reflect.String:
			argument.Type = "string"
			argument.Value = bytex.FromString(fmt.Sprintf("\"%s\"", rv.String()))
			break
		case reflect.Bool:
			argument.Type = "bool"
			if rv.Bool() {
				argument.Value = trueBytes
			} else {
				argument.Value = falseBytes
			}
			break
		case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
			argument.Type = "int"
			argument.Value, _ = json.Marshal(rv.Int())
			break
		case reflect.Float32, reflect.Float64:
			argument.Type = "float"
			argument.Value, _ = json.Marshal(rv.Float())
			break
		default:
			if rt.ConvertibleTo(datetimeType) {
				argument.Type = "datetime"
				argument.Value, _ = json.Marshal(rv.Convert(datetimeType).Interface())
				break
			} else if rt.ConvertibleTo(dateType) {
				argument.Type = "date"
				argument.Value, _ = json.Marshal(rv.Convert(dateType).Interface())
				break
			} else if rt.ConvertibleTo(timeType) {
				argument.Type = "time"
				argument.Value, _ = json.Marshal(rv.Convert(timeType).Interface())
				break
			} else if rt.ConvertibleTo(jsonDateType) {
				argument.Type = "date"
				argument.Value, _ = json.Marshal(rv.Convert(jsonDateType).Interface())
				break
			} else if rt.ConvertibleTo(jsonTimeType) {
				argument.Type = "time"
				argument.Value, _ = json.Marshal(rv.Convert(jsonTimeType).Interface())
				break
			} else if rt.ConvertibleTo(bytesType) {
				argument.Type = "bytes"
				argument.Value, _ = json.Marshal(rv.Convert(bytesType).Bytes())
				break
			} else if rt.ConvertibleTo(byteType) {
				argument.Type = "byte"
				argument.Value, _ = json.Marshal(rv.Convert(byteType).Interface())
				break
			} else if rt.ConvertibleTo(nullStringType) {
				argument.Type = "string"
				value := rv.Convert(nullStringType).Interface().(sql.NullString)
				if value.Valid {
					argument.Value = bytex.FromString(fmt.Sprintf("\"%s\"", value.String))
				} else {
					argument.Nil = true
				}
				break
			} else if rt.ConvertibleTo(nullBoolType) {
				argument.Type = "bool"
				value := rv.Convert(nullBoolType).Interface().(sql.NullBool)
				if value.Valid {
					if value.Bool {
						argument.Value = trueBytes
					} else {
						argument.Value = falseBytes
					}
				} else {
					argument.Nil = true
				}
				break
			} else if rt.ConvertibleTo(nullInt64Type) {
				argument.Type = "int"
				value := rv.Convert(nullInt64Type).Interface().(sql.NullInt64)
				if value.Valid {
					argument.Value, _ = json.Marshal(value.Int64)
				} else {
					argument.Nil = true
				}
				break
			} else if rt.ConvertibleTo(nullInt32Type) {
				argument.Type = "int"
				value := rv.Convert(nullInt32Type).Interface().(sql.NullInt32)
				if value.Valid {
					argument.Value, _ = json.Marshal(value.Int32)
				} else {
					argument.Nil = true
				}
				break
			} else if rt.ConvertibleTo(nullInt16Type) {
				argument.Type = "int"
				value := rv.Convert(nullInt16Type).Interface().(sql.NullInt16)
				if value.Valid {
					argument.Value, _ = json.Marshal(value.Int16)
				} else {
					argument.Nil = true
				}
				break
			} else if rt.ConvertibleTo(nullFloatType) {
				argument.Type = "float"
				value := rv.Convert(nullFloatType).Interface().(sql.NullFloat64)
				if value.Valid {
					argument.Value, _ = json.Marshal(value.Float64)
				} else {
					argument.Nil = true
				}
				break
			} else if rt.ConvertibleTo(nullTimeType) {
				argument.Type = "datetime"
				value := rv.Convert(nullTimeType).Interface().(sql.NullTime)
				if value.Valid {
					argument.Value, _ = json.Marshal(value.Time)
				} else {
					argument.Nil = true
				}
				break
			} else if rt.ConvertibleTo(nullByteType) {
				argument.Type = "byte"
				value := rv.Convert(nullByteType).Interface().(sql.NullByte)
				if value.Valid {
					argument.Value, _ = json.Marshal(value.Byte)
				} else {
					argument.Nil = true
				}
				break
			} else {
				if rt.Implements(jsonMarshalerType) || reflect.New(rt).Type().Implements(jsonMarshalerType) {
					p, encodeErr := json.Marshal(v)
					if encodeErr != nil {
						err = errors.Warning("sql: new argument failed").
							WithCause(fmt.Errorf("type of value implements json.Marshaler but encode failed")).
							WithCause(encodeErr).WithMeta("type", rt.String())
						return
					}
					argument.Type = "json"
					argument.Value = p
					break
				}
				err = errors.Warning("sql: new argument failed").WithCause(fmt.Errorf("type of value is not supported")).WithMeta("type", rt.String())
				return
			}
		}
		break
	}
	return
}

type Argument struct {
	Nil   bool   `json:"nil"`
	Type  string `json:"type"`
	Value []byte `json:"value"`
	Name  string `json:"name"`
}

func (argument Argument) Interface() (v any, err error) {
	if argument.Nil {
		if argument.Name != "" {
			v = sql.Named(argument.Name, nil)
		}
		return
	}
	switch argument.Type {
	case "string":
		s := ""
		if len(argument.Value) > 0 {
			_ = json.Unmarshal(argument.Value, &s)
		}
		v = s
		if argument.Name != "" {
			v = sql.Named(argument.Name, v)
		}
		break
	case "int":
		i := int64(0)
		if len(argument.Value) > 0 {
			_ = json.Unmarshal(argument.Value, &i)
		}
		v = i
		if argument.Name != "" {
			v = sql.Named(argument.Name, v)
		}
		break
	case "float":
		f := float64(0)
		if len(argument.Value) > 0 {
			_ = json.Unmarshal(argument.Value, &f)
		}
		v = f
		if argument.Name != "" {
			v = sql.Named(argument.Name, v)
		}
		break
	case "uint":
		u := uint64(0)
		if len(argument.Value) > 0 {
			_ = json.Unmarshal(argument.Value, &u)
		}
		v = u
		if argument.Name != "" {
			v = sql.Named(argument.Name, v)
		}
		break
	case "bool":
		b := false
		if len(argument.Value) > 0 {
			_ = json.Unmarshal(argument.Value, &b)
		}
		v = b
		if argument.Name != "" {
			v = sql.Named(argument.Name, v)
		}
		break
	case "json":
		v = argument.Value
		if argument.Name != "" {
			v = sql.Named(argument.Name, v)
		}
		break
	case "byte":
		b := byte(0)
		if len(argument.Value) > 0 {
			_ = json.Unmarshal(argument.Value, &b)
		}
		v = b
		if argument.Name != "" {
			v = sql.Named(argument.Name, v)
		}
	case "bytes":
		// raw
		p := sql.RawBytes{}
		if len(argument.Value) > 0 {
			_ = json.Unmarshal(argument.Value, &p)
		}
		v = p
		if argument.Name != "" {
			v = sql.Named(argument.Name, v)
		}
		break
	case "datetime":
		t := time.Time{}
		if len(argument.Value) > 0 {
			_ = json.Unmarshal(argument.Value, &t)
		}
		v = t
		if argument.Name != "" {
			v = sql.Named(argument.Name, v)
		}
		break
	case "date":
		d := times.Date{}
		if len(argument.Value) > 0 {
			_ = json.Unmarshal(argument.Value, &d)
		}
		v = d
		if argument.Name != "" {
			v = sql.Named(argument.Name, v)
		}
		break
	case "time":
		t := times.Time{}
		if len(argument.Value) > 0 {
			_ = json.Unmarshal(argument.Value, &t)
		}
		v = t
		if argument.Name != "" {
			v = sql.Named(argument.Name, v)
		}
		break
	default:
		err = errors.Warning("sql: unknown argument type").WithMeta("type", argument.Type)
		return
	}
	return
}
