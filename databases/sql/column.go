package sql

import (
	"database/sql"
	"fmt"
	"github.com/aacfactory/json"
	"reflect"
	"strings"
)

const (
	StringType   = ColumnType("string")
	IntType      = ColumnType("int")
	FloatType    = ColumnType("float")
	BytesType    = ColumnType("bytes")
	JsonType     = ColumnType("json")
	BoolType     = ColumnType("bool")
	DatetimeType = ColumnType("datetime")
	DateType     = ColumnType("date")
	TimeType     = ColumnType("time")
	UnknownType  = ColumnType("unknown")
)

type ColumnType string

type column struct {
	Type_ ColumnType      `json:"type"`
	Name_ string          `json:"name"`
	Value json.RawMessage `json:"value"`
	Nil   bool            `json:"nil"`
}

func (c *column) Type() (typ string) {
	typ = string(c.Type_)
	return
}

func (c *column) Name() (v string) {
	v = c.Name_
	return
}

func (c *column) IsNil() (ok bool) {
	ok = c.Nil
	return
}

func (c *column) Get(v interface{}) (err error) {
	if c.Nil {
		return
	}
	err = json.Unmarshal(c.Value, v)
	return
}

func (c *column) RawValue() (raw []byte) {
	raw = c.Value
	return
}

type ColumnScanner struct {
	*column
	value sql.Scanner
}

func (c *ColumnScanner) Scan(src interface{}) error {
	scanErr := c.value.Scan(src)
	if scanErr != nil {
		return scanErr
	}
	switch c.Type_ {
	case StringType:
		x := c.value.(*sql.NullString)
		if x.Valid {
			p, encodeErr := json.Marshal(x.String)
			if encodeErr != nil {
				return encodeErr
			}
			c.Value = p
		} else {
			c.Value = []byte("null")
			c.Nil = true
		}
		break
	case IntType:
		x := c.value.(*sql.NullInt64)
		if x.Valid {
			p, encodeErr := json.Marshal(x.Int64)
			if encodeErr != nil {
				return encodeErr
			}
			c.Value = p
		} else {
			c.Value = []byte("null")
			c.Nil = true
		}
		break
	case FloatType:
		x := c.value.(*sql.NullFloat64)
		if x.Valid {
			p, encodeErr := json.Marshal(x.Float64)
			if encodeErr != nil {
				return encodeErr
			}
			c.Value = p
		} else {
			c.Value = []byte("null")
			c.Nil = true
		}
		break
	case BytesType, UnknownType:
		x := c.value.(*NullSQLRaw)
		if x.Valid {
			p, encodeErr := json.Marshal(x.Raw)
			if encodeErr != nil {
				return encodeErr
			}
			c.Value = p
		} else {
			c.Value = []byte("null")
			c.Nil = true
		}
		break
	case JsonType:
		x := c.value.(*NullJson)
		if x.Valid {
			c.Value = x.Json
		} else {
			c.Value = []byte("null")
			c.Nil = true
		}
		break
	case BoolType:
		x := c.value.(*sql.NullBool)
		if x.Valid {
			p, encodeErr := json.Marshal(x.Bool)
			if encodeErr != nil {
				return encodeErr
			}
			c.Value = p
		} else {
			c.Value = []byte("null")
			c.Nil = true
		}
		break
	case DatetimeType:
		x := c.value.(*sql.NullTime)
		if x.Valid {
			p, encodeErr := json.Marshal(x.Time)
			if encodeErr != nil {
				return encodeErr
			}
			c.Value = p
		} else {
			c.Value = []byte("null")
			c.Nil = true
		}
		break
	case DateType:
		x := c.value.(*Date)
		p, encodeErr := json.Marshal(x)
		if encodeErr != nil {
			return encodeErr
		}
		c.Value = p
		break
	case TimeType:
		x := c.value.(*Time)
		p, encodeErr := json.Marshal(x)
		if encodeErr != nil {
			return encodeErr
		}
		c.Value = p
		break
	}
	return c.value.Scan(src)
}

func NewColumnScanner(ct *sql.ColumnType) (scanner *ColumnScanner) {
	colName := strings.ToUpper(ct.Name())
	_, scale, isNumber := ct.DecimalSize()
	if isNumber {
		if scale > 0 {
			scanner = &ColumnScanner{
				column: &column{
					Type_: FloatType,
					Name_: colName,
					Value: nil,
					Nil:   false,
				},
				value: &sql.NullFloat64{},
			}
		} else {
			scanner = &ColumnScanner{
				column: &column{
					Type_: IntType,
					Name_: colName,
					Value: nil,
					Nil:   false,
				},
				value: &sql.NullInt64{},
			}
		}
		return
	}

	typeName := strings.ToUpper(ct.DatabaseTypeName())

	// string
	if strings.Contains(typeName, "VARCHAR") || strings.Contains(typeName, "CHAR") || strings.Contains(typeName, "TEXT") {
		scanner = &ColumnScanner{
			column: &column{
				Type_: StringType,
				Name_: colName,
				Value: nil,
				Nil:   false,
			},
			value: &sql.NullString{},
		}
		return
	}
	// int serial
	if strings.Contains(typeName, "INT") || strings.Contains(typeName, "SERIAL") {
		scanner = &ColumnScanner{
			column: &column{
				Type_: IntType,
				Name_: colName,
				Value: nil,
				Nil:   false,
			},
			value: &sql.NullInt64{},
		}
		return
	}
	// float
	if strings.Contains(typeName, "FLOAT") || strings.Contains(typeName, "DOUBLE") {
		scanner = &ColumnScanner{
			column: &column{
				Type_: FloatType,
				Name_: colName,
				Value: nil,
				Nil:   false,
			},
			value: &sql.NullFloat64{},
		}
		return
	}
	// bool
	if strings.Contains(typeName, "BOOL") {
		scanner = &ColumnScanner{
			column: &column{
				Type_: BoolType,
				Name_: colName,
				Value: nil,
				Nil:   false,
			},
			value: &sql.NullBool{},
		}
		return
	}
	// time
	if strings.Contains(typeName, "TIMESTAMP") || strings.Contains(typeName, "DATETIME") {
		scanner = &ColumnScanner{
			column: &column{
				Type_: DatetimeType,
				Name_: colName,
				Value: nil,
				Nil:   false,
			},
			value: &sql.NullTime{},
		}
		return
	}
	// date
	if strings.Contains(typeName, "DATE") {
		scanner = &ColumnScanner{
			column: &column{
				Type_: DateType,
				Name_: colName,
				Value: nil,
				Nil:   false,
			},
			value: &Date{},
		}
		return
	}
	// time
	if strings.Contains(typeName, "TIME") {
		scanner = &ColumnScanner{
			column: &column{
				Type_: TimeType,
				Name_: colName,
				Value: nil,
				Nil:   false,
			},
			value: &Time{},
		}
		return
	}
	// json
	if strings.Contains(typeName, "JSON") || strings.Contains(typeName, "JSONB") {
		scanner = &ColumnScanner{
			column: &column{
				Type_: JsonType,
				Name_: colName,
				Value: nil,
				Nil:   false,
			},
			value: &NullJson{},
		}
		return
	}
	// bytes
	if strings.Contains(typeName, "BLOB") {
		scanner = &ColumnScanner{
			column: &column{
				Type_: BytesType,
				Name_: colName,
				Value: nil,
				Nil:   false,
			},
			value: &NullSQLRaw{},
		}
		return
	}
	scanner = &ColumnScanner{
		column: &column{
			Type_: UnknownType,
			Name_: colName,
			Value: nil,
			Nil:   false,
		},
		value: &NullSQLRaw{},
	}
	return
}

type NullJson struct {
	Json  json.RawMessage
	Valid bool
}

func (v *NullJson) Scan(src interface{}) error {
	v.Json = []byte("null")
	str := &sql.NullString{}
	scanErr := str.Scan(src)
	if scanErr != nil {
		return scanErr
	}
	if str.String == "" {
		return nil
	}
	if json.ValidateString(str.String) {
		v.Valid = true
		v.Json = []byte(str.String)
	}
	return nil
}

type NullSQLRaw struct {
	Raw   sql.RawBytes
	Valid bool
}

func (v *NullSQLRaw) Scan(src interface{}) error {
	if src == nil {
		return nil
	}
	switch src.(type) {
	case string:
		x := src.(string)
		if x == "" {
			return nil
		}
		v.Raw = []byte(x)
		v.Valid = true
	case []byte:
		x := src.([]byte)
		if len(x) > 0 {
			v.Raw = x
			v.Valid = true
		}
	default:
		return fmt.Errorf("scan sql raw value failed for %v is not supported", reflect.TypeOf(src).String())
	}

	return nil
}

type FieldColumn struct {
	Kind      string
	FieldType reflect.Type
	Column    *Column
}
