package sql

import (
	"github.com/aacfactory/avro"
	"github.com/aacfactory/errors"
	"github.com/aacfactory/fns/commons/times"
	"reflect"
	"sync"
	"time"
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
	Name         string `json:"name" avro:"name"`
	DatabaseType string `json:"databaseType" avro:"databaseType"`
	Type         string `json:"type" avro:"type"`
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
	Valid bool            `json:"valid" avro:"valid"`
	Value avro.RawMessage `json:"value" avro:"value"`
}

func (c *Column) Scan(src any) (err error) {
	if src == nil {
		return
	}
	c.Value, err = avro.Marshal(src)
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
		err = avro.Unmarshal(p, &v)
		if err != nil {
			err = errors.Warning("sql: value of column is not string")
			return
		}
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
		err = avro.Unmarshal(p, &v)
		if err != nil {
			err = errors.Warning("sql: value of column is not bool")
			return
		}
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
		err = avro.Unmarshal(p, &v)
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
		err = avro.Unmarshal(p, &v)
		if err != nil {
			err = errors.Warning("sql: value of column is not float")
			return
		}
	}
	return
}

func (c *Column) Datetime() (v time.Time, err error) {
	if c.Valid {
		p := c.Value
		pLen := len(p)
		if pLen == 0 {
			return
		}
		err = avro.Unmarshal(p, &v)
		if err != nil {
			err = errors.Warning("sql: value of column is not datetime")
			return
		}
	}
	return
}

func (c *Column) Date() (v times.Date, err error) {
	if c.Valid {
		p := c.Value
		pLen := len(p)
		if pLen == 0 {
			return
		}
		t := time.Time{}
		err = avro.Unmarshal(p, &t)
		if err != nil {
			err = errors.Warning("sql: value of column is not date")
			return
		}
		v = times.DataOf(t)
	}
	return
}

func (c *Column) Time() (v times.Time, err error) {
	if c.Valid {
		p := c.Value
		pLen := len(p)
		if pLen == 0 {
			return
		}
		t := time.Time{}
		err = avro.Unmarshal(p, &t)
		if err != nil {
			err = errors.Warning("sql: value of column is not time")
			return
		}
		v = times.TimeOf(t)
	}
	return
}

func (c *Column) Bytes() (v []byte, err error) {
	if c.Valid {
		p := c.Value
		pLen := len(p)
		if pLen == 0 {
			return
		}
		err = avro.Unmarshal(c.Value, &v)
		if err != nil {
			err = errors.Warning("sql: value of column is not bytes")
			return
		}
	}
	return
}

func (c *Column) Byte() (v byte, err error) {
	if c.Valid {
		p := c.Value
		pLen := len(p)
		if pLen == 0 {
			return
		}
		err = avro.Unmarshal(c.Value, &v)
		if err != nil {
			err = errors.Warning("sql: value of column is not byte")
			return
		}
	}
	return
}

func (c *Column) Reset() {
	c.Valid = false
	c.Value = nil
}

var (
	columnsPool = sync.Pool{}
)

type Columns []any

func newMultiColumns(size int) *multiColumns {
	return &multiColumns{
		size:   size,
		values: nil,
	}
}

type multiColumns struct {
	size   int
	values []Columns
}

func (mc *multiColumns) Next() (columns Columns) {
	cached := columnsPool.Get()
	if cached == nil {
		columns = make([]any, mc.size)
		for i := 0; i < mc.size; i++ {
			columns[i] = &Column{}
		}
		mc.values = append(mc.values, columns)
		return
	}
	columns = cached.(Columns)
	cLen := len(columns)
	if delta := mc.size - cLen; delta < 0 {
		columns = columns[0:mc.size]
	} else if delta > 0 {
		for i := 0; i < delta; i++ {
			columns = append(columns, &Column{})
		}
	}
	mc.values = append(mc.values, columns)
	return
}

func (mc *multiColumns) Rows() (rows []Row) {
	vLen := len(mc.values)
	if vLen == 0 {
		return
	}
	rows = make([]Row, vLen)
	for i := 0; i < vLen; i++ {
		row := make(Row, mc.size)
		columns := mc.values[i]
		for j, column := range columns {
			row[j] = *(column.(*Column))
		}
		rows[i] = row
	}
	return
}

func (mc *multiColumns) Release() {
	for _, value := range mc.values {
		for _, v := range value {
			v.(*Column).Reset()
		}
		columnsPool.Put(value)
	}
	mc.values = nil
}
