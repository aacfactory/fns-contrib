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

const (
	columnStructTag = "col"
)

type ColumnType string

type Column struct {
	Type      ColumnType      `json:"type,omitempty"`
	Name      string          `json:"name,omitempty"`
	Value     json.RawMessage `json:"value,omitempty"`
	Nil       bool            `json:"nil,omitempty"`
	scanValue db.Scanner
}

func (c *Column) Scan(src interface{}) error {
	scanErr := c.scanValue.Scan(src)
	if scanErr != nil {
		return scanErr
	}
	switch c.Type {
	case StringType:
		x := c.scanValue.(*db.NullString)
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
		x := c.scanValue.(*db.NullInt64)
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
		x := c.scanValue.(*db.NullFloat64)
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
		x := c.scanValue.(*NullSQLRaw)
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
		x := c.scanValue.(*NullJson)
		if x.Valid {
			c.Value = x.Json
		} else {
			c.Value = []byte("null")
			c.Nil = true
		}
	case BoolType:
		x := c.scanValue.(*db.NullBool)
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
		x := c.scanValue.(*db.NullTime)
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
	return c.scanValue.Scan(src)
}

func createColumnValueByColumnType(ct *db.ColumnType) (col *Column) {
	colName := ct.Name()
	_, scale, isNumber := ct.DecimalSize()
	if isNumber {
		if scale > 0 {
			col = &Column{
				Type:      FloatType,
				Name:      colName,
				Value:     nil,
				Nil:       false,
				scanValue: &db.NullFloat64{},
			}
		} else {
			col = &Column{
				Type:      IntType,
				Name:      colName,
				Value:     nil,
				Nil:       false,
				scanValue: &db.NullInt64{},
			}
		}
		return
	}

	typeName := strings.ToUpper(ct.DatabaseTypeName())

	// string
	if strings.Contains(typeName, "VARCHAR") || strings.Contains(typeName, "CHAR") || strings.Contains(typeName, "TEXT") {
		col = &Column{
			Type:      StringType,
			Name:      colName,
			Value:     nil,
			Nil:       false,
			scanValue: &db.NullString{},
		}
		return
	}
	// int serial
	if strings.Contains(typeName, "INT") || strings.Contains(typeName, "SERIAL") {
		col = &Column{
			Type:      IntType,
			Name:      colName,
			Value:     nil,
			Nil:       false,
			scanValue: &db.NullInt64{},
		}
		return
	}
	// float
	if strings.Contains(typeName, "FLOAT") || strings.Contains(typeName, "DOUBLE") {
		col = &Column{
			Type:      FloatType,
			Name:      colName,
			Value:     nil,
			Nil:       false,
			scanValue: &db.NullFloat64{},
		}
		return
	}
	// bool
	if strings.Contains(typeName, "BOOL") {
		col = &Column{
			Type:      BoolType,
			Name:      colName,
			Value:     nil,
			Nil:       false,
			scanValue: &db.NullBool{},
		}
		return
	}
	// time
	if strings.Contains(typeName, "TIMESTAMP") || strings.Contains(typeName, "DATE") || strings.Contains(typeName, "TIME") {
		col = &Column{
			Type:      TimeType,
			Name:      colName,
			Value:     nil,
			Nil:       false,
			scanValue: &db.NullTime{},
		}
		return
	}
	// json
	if strings.Contains(typeName, "JSON") || strings.Contains(typeName, "JSONB") {
		col = &Column{
			Type:      JsonType,
			Name:      colName,
			Value:     nil,
			Nil:       false,
			scanValue: &NullJson{},
		}
		return
	}
	// bytes
	if strings.Contains(typeName, "BLOB") {
		col = &Column{
			Type:      BytesType,
			Name:      colName,
			Value:     nil,
			Nil:       false,
			scanValue: &NullJson{},
		}
		return
	}

	col = &Column{
		Type:      UnknownType,
		Name:      colName,
		Value:     nil,
		Nil:       false,
		scanValue: &NullSQLRaw{},
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
