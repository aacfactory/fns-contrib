package sql

import (
	db "database/sql"
	"fmt"
	"github.com/aacfactory/json"
	"reflect"
	"strings"
)

const (
	StringType  = ColumnType("string")
	IntType     = ColumnType("int")
	FloatType   = ColumnType("float")
	BytesType   = ColumnType("bytes")
	JsonType    = ColumnType("json")
	BoolType    = ColumnType("bool")
	TimeType    = ColumnType("time")
	UnknownType = ColumnType("unknown")
)

type ColumnType string

type Column struct {
	Type  ColumnType      `json:"type,omitempty"`
	Name  string          `json:"name,omitempty"`
	Value json.RawMessage `json:"value,omitempty"`
	Nil   bool            `json:"nil,omitempty"`
}

func (c *Column) Decode(v interface{}) (err error) {
	if c.Nil {
		return
	}
	err = json.Unmarshal(c.Value, v)
	return
}

type ColumnScanner struct {
	Column
	value db.Scanner
}

func (c *ColumnScanner) Scan(src interface{}) error {
	scanErr := c.value.Scan(src)
	if scanErr != nil {
		return scanErr
	}
	switch c.Type {
	case StringType:
		x := c.value.(*db.NullString)
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
	case IntType:
		x := c.value.(*db.NullInt64)
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
	case FloatType:
		x := c.value.(*db.NullFloat64)
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
	case JsonType:
		x := c.value.(*NullJson)
		if x.Valid {
			c.Value = x.Json
		} else {
			c.Value = []byte("null")
			c.Nil = true
		}
	case BoolType:
		x := c.value.(*db.NullBool)
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
	case TimeType:
		x := c.value.(*db.NullTime)
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
	}
	return c.value.Scan(src)
}

func NewColumnScanner(ct *db.ColumnType) (scanner *ColumnScanner) {
	colName := strings.ToUpper(ct.Name())
	_, scale, isNumber := ct.DecimalSize()
	if isNumber {
		if scale > 0 {
			scanner = &ColumnScanner{
				Column: Column{
					Type:  FloatType,
					Name:  colName,
					Value: nil,
					Nil:   false,
				},
				value: &db.NullFloat64{},
			}
		} else {
			scanner = &ColumnScanner{
				Column: Column{
					Type:  IntType,
					Name:  colName,
					Value: nil,
					Nil:   false,
				},
				value: &db.NullInt64{},
			}
		}
		return
	}

	typeName := strings.ToUpper(ct.DatabaseTypeName())

	// string
	if strings.Contains(typeName, "VARCHAR") || strings.Contains(typeName, "CHAR") || strings.Contains(typeName, "TEXT") {
		scanner = &ColumnScanner{
			Column: Column{
				Type:  StringType,
				Name:  colName,
				Value: nil,
				Nil:   false,
			},
			value: &db.NullString{},
		}
		return
	}
	// int serial
	if strings.Contains(typeName, "INT") || strings.Contains(typeName, "SERIAL") {
		scanner = &ColumnScanner{
			Column: Column{
				Type:  IntType,
				Name:  colName,
				Value: nil,
				Nil:   false,
			},
			value: &db.NullInt64{},
		}
		return
	}
	// float
	if strings.Contains(typeName, "FLOAT") || strings.Contains(typeName, "DOUBLE") {
		scanner = &ColumnScanner{
			Column: Column{
				Type:  FloatType,
				Name:  colName,
				Value: nil,
				Nil:   false,
			},
			value: &db.NullFloat64{},
		}
		return
	}
	// bool
	if strings.Contains(typeName, "BOOL") {
		scanner = &ColumnScanner{
			Column: Column{
				Type:  BoolType,
				Name:  colName,
				Value: nil,
				Nil:   false,
			},
			value: &db.NullBool{},
		}
		return
	}
	// time
	if strings.Contains(typeName, "TIMESTAMP") || strings.Contains(typeName, "DATE") || strings.Contains(typeName, "TIME") {
		scanner = &ColumnScanner{
			Column: Column{
				Type:  TimeType,
				Name:  colName,
				Value: nil,
				Nil:   false,
			},
			value: &db.NullTime{},
		}
		return
	}
	// json
	if strings.Contains(typeName, "JSON") || strings.Contains(typeName, "JSONB") {
		scanner = &ColumnScanner{
			Column: Column{
				Type:  JsonType,
				Name:  colName,
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
			Column: Column{
				Type:  BytesType,
				Name:  colName,
				Value: nil,
				Nil:   false,
			},
			value: &NullSQLRaw{},
		}
		return
	}
	scanner = &ColumnScanner{
		Column: Column{
			Type:  UnknownType,
			Name:  colName,
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
	str := &db.NullString{}
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
	Raw   db.RawBytes
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
