package sql

import (
	"bytes"
	"database/sql"
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

func NewColumn(src any) (c Column, err error) {
	if src == nil {
		return
	}
	c, err = json.Marshal(src)
	if err != nil {
		err = errors.Warning("sql: new column failed").WithCause(err)
		return
	}
	return
}

type Column []byte

func (c *Column) UnmarshalJSON(p []byte) error {
	r := json.RawMessage(*c)
	err := r.UnmarshalJSON(p)
	if err != nil {
		return err
	}
	*c = append((*c)[0:0], r...)
	return nil
}

func (c *Column) MarshalJSON() ([]byte, error) {
	return json.RawMessage(*c).MarshalJSON()
}

func (c *Column) Len() int {
	return len(*c)
}

func (c *Column) IsNil() bool {
	p := *c
	return len(p) == 0 || bytes.Equal(p, nullBytes)
}

func (c *Column) String() (v string, err error) {
	p := *c
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

func (c *Column) Bool() (v bool, err error) {
	p := *c
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

func (c *Column) Int() (v int64, err error) {
	p := *c
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

func (c *Column) Uint() (v uint64, err error) {
	p := *c
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

func (c *Column) Float() (v float64, err error) {
	p := *c
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

func (c *Column) Datetime() (v time.Time, err error) {
	str, strErr := c.String()
	if strErr != nil {
		err = errors.Warning("sql: value of column is not datetime")
		return
	}
	v, err = time.Parse(time.RFC3339, str)
	if err != nil {
		err = errors.Warning("sql: value of column is not datetime")
		return
	}
	return
}

func (c *Column) Date() (v times.Date, err error) {
	p := *c
	pLen := len(p)
	if pLen == 0 {
		return
	}
	err = json.Unmarshal(p, &v)
	if err != nil {
		err = errors.Warning("sql: value of column is not date")
		return
	}
	return
}

func (c *Column) Time() (v times.Time, err error) {
	p := *c
	pLen := len(p)
	if pLen == 0 {
		return
	}
	err = json.Unmarshal(p, &v)
	if err != nil {
		err = errors.Warning("sql: value of column is not time")
		return
	}
	return
}

func (c *Column) Json() (v []byte, err error) {
	p := *c
	pLen := len(p)
	if pLen == 0 {
		return
	}
	if json.Validate(p) {
		v = p
		return
	}
	err = errors.Warning("sql: value of column is not json")
	return
}

func (c *Column) Bytes() (v []byte, err error) {
	p := *c
	pLen := len(p)
	if pLen == 0 {
		return
	}
	if json.Validate(p) {
		v = p
		return
	}
	err = json.Unmarshal(p, &v)
	if err != nil {
		err = errors.Warning("sql: value of column is not bytes")
		return
	}
	return
}

func (c *Column) Byte() (v byte, err error) {
	p := *c
	pLen := len(p)
	if pLen == 0 {
		return
	}
	err = json.Unmarshal(p, &v)
	if err != nil {
		err = errors.Warning("sql: value of column is not byte")
		return
	}
	return
}

func (c *Column) Scan(dst any) (err error) {
	p := *c
	pLen := len(p)
	switch d := dst.(type) {
	case *sql.NullBool:
		if pLen == 0 || bytes.Equal(p, nullBytes) {
			break
		}
		d.Valid = true
		d.Bool = bytes.Equal(p, trueBytes)
		break
	case *sql.NullString:
		if pLen == 0 || bytes.Equal(p, nullBytes) {
			break
		}
		d.Valid = true
		d.String = bytex.ToString(p[1 : pLen-1])
		break
	case *sql.NullTime:
		if pLen == 0 || bytes.Equal(p, nullBytes) {
			break
		}
		d.Valid = true
		err = json.Unmarshal(p, &d.Time)
		break
	case *sql.NullInt16:
		if pLen == 0 || bytes.Equal(p, nullBytes) {
			break
		}
		d.Valid = true
		err = json.Unmarshal(p, &d.Int16)
		break
	case *sql.NullInt32:
		if pLen == 0 || bytes.Equal(p, nullBytes) {
			break
		}
		d.Valid = true
		err = json.Unmarshal(p, &d.Int32)
		break
	case *sql.NullInt64:
		if pLen == 0 || bytes.Equal(p, nullBytes) {
			break
		}
		d.Valid = true
		err = json.Unmarshal(p, &d.Int64)
		break
	case *sql.NullFloat64:
		if pLen == 0 || bytes.Equal(p, nullBytes) {
			break
		}
		d.Valid = true
		err = json.Unmarshal(p, &d.Float64)
		break
	case *sql.NullByte:
		if pLen == 0 || bytes.Equal(p, nullBytes) {
			break
		}
		d.Valid = true
		err = json.Unmarshal(p, &d.Byte)
		break
	default:
		err = json.Unmarshal(p, dst)
		break
	}
	if err != nil {
		err = errors.Warning("sql: column scan failed").WithCause(err)
		return
	}
	return
}
