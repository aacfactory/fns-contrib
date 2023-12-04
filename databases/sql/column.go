package sql

import (
	"bytes"
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
	switch scanType.Kind() {
	case reflect.String:
		v = ColumnType{
			Name:         name,
			DatabaseType: databaseType,
			Type:         "string",
		}
		break
	case reflect.Bool:
		v = ColumnType{
			Name:         name,
			DatabaseType: databaseType,
			Type:         "bool",
		}
		break
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		v = ColumnType{
			Name:         name,
			DatabaseType: databaseType,
			Type:         "int",
		}
		break
	case reflect.Float32, reflect.Float64:
		v = ColumnType{
			Name:         name,
			DatabaseType: databaseType,
			Type:         "float",
		}
		break
	case reflect.Uint8:
		v = ColumnType{
			Name:         name,
			DatabaseType: databaseType,
			Type:         "byte",
		}
		break
	default:
		if scanType.ConvertibleTo(datetimeType) {
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
		break
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

type Column struct {
	Valid bool   `json:"valid"`
	Value []byte `json:"value"`
}

func (c *Column) Scan(src any) (err error) {
	if src == nil {
		return
	}
	c.Value, err = json.Marshal(src)
	if err != nil {
		err = errors.Warning("sql: column scan failed").WithCause(err)
		return
	}
	c.Valid = true
	return
}

func (c *Column) String() (v string, err error) {
	if c.Valid {
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
	}
	return
}

func (c *Column) Bool() (v bool, err error) {
	if c.Valid {
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
	}
	return
}

func (c *Column) Int() (v int64, err error) {
	if c.Valid {
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
	}
	return
}

func (c *Column) Float() (v float64, err error) {
	if c.Valid {
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
	}
	return
}

func (c *Column) Datetime() (v time.Time, err error) {
	if c.Valid {
		err = json.Unmarshal(c.Value, &v)
	}
	return
}

func (c *Column) Date() (v times.Date, err error) {
	if c.Valid {
		t := time.Time{}
		err = json.Unmarshal(c.Value, &t)
		if err != nil {
			return
		}
		v = times.DataOf(t)
	}
	return
}

func (c *Column) Time() (v times.Time, err error) {
	if c.Valid {
		t := time.Time{}
		err = json.Unmarshal(c.Value, &t)
		if err != nil {
			return
		}
		v = times.TimeOf(t)
	}
	return
}

func (c *Column) Bytes() (v []byte, err error) {
	if c.Valid {
		err = json.Unmarshal(c.Value, &v)
	}
	return
}

func (c *Column) Byte() (v byte, err error) {
	if c.Valid {
		err = json.Unmarshal(c.Value, &v)
	}
	return
}
