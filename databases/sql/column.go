package sql

import (
	"bytes"
	"fmt"
	"github.com/aacfactory/errors"
	"github.com/aacfactory/fns/commons/bytex"
	"github.com/aacfactory/fns/commons/times"
	"github.com/aacfactory/json"
	"reflect"
	"strconv"
	"time"
	"unsafe"
)

func NewColumnType(name string, databaseType string, scanType reflect.Type) (v ColumnType) {
	if scanType == anyType {
		v = ColumnType{
			Name:         name,
			DatabaseType: databaseType,
			Type:         "bytes",
		}
		return
	}
	if scanType == stringType {
		v = ColumnType{
			Name:         name,
			DatabaseType: databaseType,
			Type:         "string",
		}
	} else if scanType == boolType {
		v = ColumnType{
			Name:         name,
			DatabaseType: databaseType,
			Type:         "bool",
		}
	} else if scanType.ConvertibleTo(intType) {
		v = ColumnType{
			Name:         name,
			DatabaseType: databaseType,
			Type:         "int",
		}
	} else if scanType.ConvertibleTo(floatType) {
		v = ColumnType{
			Name:         name,
			DatabaseType: databaseType,
			Type:         "float",
		}
	} else if scanType.ConvertibleTo(datetimeType) {
		typeName := "datetime"
		if databaseType == "TIME" {
			typeName = "time"
		} else if databaseType == "DATE" {
			typeName = "date"
		}
		v = ColumnType{
			Name:         name,
			DatabaseType: databaseType,
			Type:         typeName,
		}
	} else if scanType.ConvertibleTo(bytesType) {
		v = ColumnType{
			Name:         name,
			DatabaseType: databaseType,
			Type:         "bytes",
		}
	} else if scanType.ConvertibleTo(byteType) {
		v = ColumnType{
			Name:         name,
			DatabaseType: databaseType,
			Type:         "byte",
		}
	} else {
		v = ColumnType{
			Name:         name,
			DatabaseType: databaseType,
			Type:         "bytes",
		}
	}
	return
}

type ColumnType struct {
	Name         string `json:"name"`
	DatabaseType string `json:"databaseType"`
	Type         string `json:"type"`
}

func (ct ColumnType) ScanValue() (sv any) {
	switch ct.Type {
	case "string":
		sv = ""
		break
	case "bool":
		sv = false
		break
	case "int":
		sv = int64(0)
		break
	case "float":
		sv = float64(0)
		break
	case "datetime":
		sv = time.Time{}
		break
	case "date":
		sv = times.Date{}
		break
	case "time":
		sv = times.Time{}
		break
	case "byte":
		sv = byte(0)
		break
	default:
		sv = []byte{}
		break
	}
	return
}

func (ct ColumnType) Value(v any) (c Column, err error) {
	if v == nil {
		c = Column{
			Nil:   true,
			Value: nullBytes,
		}
		return
	}
	rv := reflect.Indirect(reflect.ValueOf(v))
	if rv.IsNil() {
		c = Column{
			Nil:   true,
			Value: nullBytes,
		}
		return
	}
	vi := rv.Interface()
	var s any
	switch ct.Type {
	case "string":
		vv, ok := vi.(string)
		if ok {
			s = vv
			break

		}
		err = errors.Warning("sql: column type make value failed").
			WithCause(fmt.Errorf("value is not string")).
			WithMeta("name", ct.Name).WithMeta("databaseType", ct.DatabaseType).WithMeta("type", ct.Type)
		return
	case "bool":
		vv, ok := vi.(bool)
		if ok {
			s = vv
			break
		}
		err = errors.Warning("sql: column type make value failed").
			WithCause(fmt.Errorf("value is not bool")).
			WithMeta("name", ct.Name).WithMeta("databaseType", ct.DatabaseType).WithMeta("type", ct.Type)
		return
	case "int":
		i, ok := vi.(int)
		if ok {
			s = i
			break
		}
		i8, i8ok := vi.(int8)
		if i8ok {
			s = i8
			break
		}
		i16, i16ok := vi.(int16)
		if i16ok {
			s = i16
			break
		}
		i32, i32ok := vi.(int32)
		if i32ok {
			s = i32
			break
		}
		i64, i64ok := vi.(int64)
		if i64ok {
			s = i64
			break
		}
		err = errors.Warning("sql: column type make value failed").
			WithCause(fmt.Errorf("value is not int")).
			WithMeta("name", ct.Name).WithMeta("databaseType", ct.DatabaseType).WithMeta("type", ct.Type)
		return
	case "float":
		f32, f32ok := vi.(float32)
		if f32ok {
			s = f32
			break
		}
		f64, f64ok := vi.(float64)
		if f64ok {
			s = f64
			break
		}
		err = errors.Warning("sql: column type make value failed").
			WithCause(fmt.Errorf("value is not float")).
			WithMeta("name", ct.Name).WithMeta("databaseType", ct.DatabaseType).WithMeta("type", ct.Type)
		return
	case "datetime":
		t, ok := vi.(time.Time)
		if ok {
			s = t
			break
		}
		err = errors.Warning("sql: column type make value failed").
			WithCause(fmt.Errorf("value is not time.Time")).
			WithMeta("name", ct.Name).WithMeta("databaseType", ct.DatabaseType).WithMeta("type", ct.Type)
		return
	case "date":
		t, ok := vi.(time.Time)
		if ok {
			s = times.DataOf(t)
			break
		}
		err = errors.Warning("sql: column type make value failed").
			WithCause(fmt.Errorf("value is not time.Time")).
			WithMeta("name", ct.Name).WithMeta("databaseType", ct.DatabaseType).WithMeta("type", ct.Type)
		return
	case "time":
		t, ok := vi.(time.Time)
		if ok {
			s = times.TimeOf(t)
			break
		}
		err = errors.Warning("sql: column type make value failed").
			WithCause(fmt.Errorf("value is not time.Time")).
			WithMeta("name", ct.Name).WithMeta("databaseType", ct.DatabaseType).WithMeta("type", ct.Type)
		return
	case "byte":
		b, ok := vi.(byte)
		if ok {
			s = b
			break
		}
		err = errors.Warning("sql: column type make value failed").
			WithCause(fmt.Errorf("value is not byte")).
			WithMeta("name", ct.Name).WithMeta("databaseType", ct.DatabaseType).WithMeta("type", ct.Type)
		return
	case "bytes":
		p, ok := vi.([]byte)
		if ok {
			if json.Validate(p) {
				s = json.RawMessage(p)
				break
			}
			s = p
			break
		}
		err = errors.Warning("sql: column type make value failed").
			WithCause(fmt.Errorf("value is not bytes")).
			WithMeta("name", ct.Name).WithMeta("databaseType", ct.DatabaseType).WithMeta("type", ct.Type)
		return
	default:
		err = errors.Warning("sql: column type make value failed").
			WithCause(fmt.Errorf("%s is unsupported", ct.Type)).
			WithMeta("name", ct.Name).WithMeta("databaseType", ct.DatabaseType).WithMeta("type", ct.Type)
		return
	}
	value, encodeErr := json.Marshal(s)
	if encodeErr != nil {
		err = errors.Warning("sql: column type make value failed").
			WithCause(encodeErr).
			WithMeta("name", ct.Name).WithMeta("databaseType", ct.DatabaseType).WithMeta("type", ct.Type)
		return
	}
	c = Column{
		Nil:   false,
		Value: value,
	}
	return
}

type Column struct {
	Nil   bool   `json:"nil"`
	Value []byte `json:"value"`
}

func (c Column) Len() int {
	return len(c.Value)
}

func (c Column) IsNil() bool {
	return c.Nil
}

func (c Column) String() (v string, err error) {
	p := c.Value
	pLen := len(p)
	if pLen == 0 {
		return
	}
	if p[0] != '"' || p[pLen-1] != '"' {
		err = errors.Warning("sql: value of column is not string")
		return
	}
	v = bytex.ToString(p[1 : pLen-1])
	return
}

func (c Column) Bool() (v bool, err error) {
	p := c.Value
	pLen := len(p)
	if pLen == 0 {
		return
	}
	v = bytes.Equal(p, trueBytes)
	if v {
		return
	}
	v = bytes.Equal(p, falseBytes)
	if v {
		v = !v
		return
	}
	err = errors.Warning("sql: value of column is not bool")
	return
}

func (c Column) Int() (v int64, err error) {
	p := c.Value
	pLen := len(p)
	if pLen == 0 {
		return
	}
	v, err = strconv.ParseInt(unsafe.String(unsafe.SliceData(p), pLen), 10, 64)
	if err != nil {
		err = errors.Warning("sql: value of column is not int")
		return
	}
	return
}

func (c Column) Uint() (v uint64, err error) {
	p := c.Value
	pLen := len(p)
	if pLen == 0 {
		return
	}
	v, err = strconv.ParseUint(unsafe.String(unsafe.SliceData(p), pLen), 10, 64)
	if err != nil {
		err = errors.Warning("sql: value of column is not uint")
		return
	}
	return
}

func (c Column) Float() (v float64, err error) {
	p := c.Value
	pLen := len(p)
	if pLen == 0 {
		return
	}
	v, err = strconv.ParseFloat(unsafe.String(unsafe.SliceData(p), pLen), 64)
	if err != nil {
		err = errors.Warning("sql: value of column is not float")
		return
	}
	return
}

func (c Column) Datetime() (v time.Time, err error) {
	err = json.Unmarshal(c.Value, &v)
	return
}

func (c Column) Date() (v times.Date, err error) {
	err = json.Unmarshal(c.Value, &v)
	return
}

func (c Column) Time() (v times.Time, err error) {
	err = json.Unmarshal(c.Value, &v)
	return
}

func (c Column) Json() (v []byte, err error) {
	v = c.Value
	return
}

func (c Column) Bytes() (v []byte, err error) {
	v = c.Value
	return
}

func (c Column) Byte() (v byte, err error) {
	err = json.Unmarshal(c.Value, &v)
	return
}
