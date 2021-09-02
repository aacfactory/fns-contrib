package sql

import (
	"database/sql"
	"fmt"
	"github.com/aacfactory/json"
	"reflect"
	"strings"
	"time"
)

func NewRows(raws *sql.Rows) (r *Rows, err error) {
	colNames, colNamesErr := raws.Columns()
	if colNamesErr != nil {
		err = colNamesErr
		return
	}
	colTypes, colTypesErr := raws.ColumnTypes()
	if colTypesErr != nil {
		err = colTypesErr
		return
	}

	values := make([]interface{}, 0, 1)
	for _, colType := range colTypes {
		values = append(values, reflect.New(colType.ScanType()))
	}

	rows := make([]*Row, 0, 1)
	for raws.Next() {
		scanErr := raws.Scan(values...)
		if scanErr != nil {
			err = scanErr
			return
		}
		columns := make([]*Column, 0, 1)
		for i := 0; i < len(colNames); i++ {
			col := &Column{}
			col.Name = colNames[i]
			colType := colTypes[i]
			scanType := colType.ScanType()
			switch scanType {
			case sqlNullStringType:
				col.Type = StringType
				v := values[i].(*sql.NullString)
				p, _ := json.Marshal(v.String)
				col.Value = p
				col.Nil = !v.Valid
			case sqlStringType:
				col.Type = StringType
				v := values[i].(*string)
				p, _ := json.Marshal(*v)
				col.Value = p
				col.Nil = false
			case sqlNullByteType:
				col.Type = ByteType
				v := values[i].(*sql.NullByte)
				p, _ := json.Marshal(v.Byte)
				col.Value = p
				col.Nil = !v.Valid
			case sqlByteType:
				col.Type = ByteType
				v := values[i].(*byte)
				p, _ := json.Marshal(*v)
				col.Value = p
				col.Nil = false
			case sqlIntType:
				col.Type = IntType
				v := values[i].(*int)
				p, _ := json.Marshal(*v)
				col.Value = p
				col.Nil = false
			case sqlInt8Type:
				col.Type = IntType
				v := values[i].(*int8)
				p, _ := json.Marshal(*v)
				col.Value = p
				col.Nil = false
			case sqlNullInt16Type:
				col.Type = IntType
				v := values[i].(*sql.NullInt16)
				p, _ := json.Marshal(v.Int16)
				col.Value = p
				col.Nil = !v.Valid
			case sqlInt16Type:
				col.Type = IntType
				v := values[i].(*int16)
				p, _ := json.Marshal(*v)
				col.Value = p
				col.Nil = false
			case sqlNullInt32Type:
				col.Type = IntType
				v := values[i].(*sql.NullInt32)
				p, _ := json.Marshal(v.Int32)
				col.Value = p
				col.Nil = !v.Valid
			case sqlInt32Type:
				col.Type = IntType
				v := values[i].(*int32)
				p, _ := json.Marshal(*v)
				col.Value = p
				col.Nil = false
			case sqlNullInt64Type:
				col.Type = IntType
				v := values[i].(*sql.NullInt64)
				p, _ := json.Marshal(v.Int64)
				col.Value = p
				col.Nil = !v.Valid
			case sqlInt64Type:
				col.Type = IntType
				v := values[i].(*int64)
				p, _ := json.Marshal(*v)
				col.Value = p
				col.Nil = false
			case sqlNullFloat64Type:
				col.Type = FloatType
				v := values[i].(*sql.NullFloat64)
				p, _ := json.Marshal(v.Float64)
				col.Value = p
				col.Nil = !v.Valid
			case sqlFloat64Type:
				col.Type = FloatType
				v := values[i].(*float64)
				p, _ := json.Marshal(*v)
				col.Value = p
				col.Nil = false
			case sqlFloat32Type:
				col.Type = FloatType
				v := values[i].(*float32)
				p, _ := json.Marshal(*v)
				col.Value = p
				col.Nil = false
			case sqlNullBoolType:
				col.Type = BoolType
				v := values[i].(*sql.NullBool)
				p, _ := json.Marshal(v.Bool)
				col.Value = p
				col.Nil = !v.Valid
			case sqlBoolType:
				col.Type = BoolType
				v := values[i].(*bool)
				p, _ := json.Marshal(*v)
				col.Value = p
				col.Nil = false
			case sqlNullTimeType:
				col.Type = TimeType
				v := values[i].(*sql.NullTime)
				p, _ := json.Marshal(v.Time)
				col.Value = p
				col.Nil = !v.Valid
			case sqlTimeType:
				col.Type = TimeType
				v := values[i].(*time.Time)
				p, _ := json.Marshal(*v)
				col.Value = p
				col.Nil = false
			case sqlRawType:
				col.Type = BytesType
				v := values[i].(*sql.RawBytes)
				p, _ := json.Marshal(*v)
				col.Value = p
				col.Nil = false
			case sqlBytesType:
				col.Type = BytesType
				v := values[i].(*[]byte)
				p, _ := json.Marshal(*v)
				col.Value = p
				col.Nil = false
			default:
				col.Type = BytesType
				v := values[i].(*sql.RawBytes)
				p, _ := json.Marshal(*v)
				col.Value = p
				col.Nil = false
			}
			columns = append(columns, col)
		}
		rows = append(rows, &Row{
			Columns: columns,
		})
	}
	return
}

type Rows struct {
	Values []*Row `json:"values,omitempty"`
}

func (r *Rows) Empty() (ok bool) {
	ok = r.Values == nil || len(r.Values) == 0
	return
}

func (r *Rows) Size() int {
	if r.Empty() {
		return 0
	}
	return len(r.Values)
}

func (r *Rows) Scan(v interface{}) (err error) {
	if v == nil {
		err = fmt.Errorf("fns SQL Rows: scan at a nil point value")
		return
	}
	typ := reflect.TypeOf(v)
	if typ.Kind() != reflect.Ptr {
		err = fmt.Errorf("fns SQL Rows: target is not ptr")
		return
	}
	if typ.Elem().Kind() != reflect.Slice {
		err = fmt.Errorf("fns SQL Rows: target elem is not slice")
		return
	}

	var elemType reflect.Type
	elemIsPtr := false
	elem := typ.Elem().Elem()
	if elem.Kind() == reflect.Ptr {
		if elem.Elem().Kind() != reflect.Struct {
			err = fmt.Errorf("fns SQL Rows: element of target is not struct or ptr of struct")
			return
		}
		elemIsPtr = true
		elemType = elem.Elem()
	} else if elem.Kind() == reflect.Struct {
		elemIsPtr = false
		elemType = elem
	} else {
		err = fmt.Errorf("fns SQL Rows: element of target is not struct or ptr of struct")
		return
	}

	rv := reflect.ValueOf(v)
	for _, value := range r.Values {
		x := reflect.New(elemType)
		err = value.Scan(x.Interface())
		if err != nil {
			return
		}
		if elemIsPtr {
			rv = reflect.Append(rv, x)
		} else {
			rv = reflect.Append(rv, x.Elem())
		}
	}
	return
}

const (
	StringType = ColumnType("string")
	IntType    = ColumnType("int")
	FloatType  = ColumnType("float")
	ByteType   = ColumnType("byte")
	BytesType  = ColumnType("bytes")
	BoolType   = ColumnType("bool")
	TimeType   = ColumnType("time")
)

const (
	columnStructTag = "col"
)

var (
	sqlNullStringType  = reflect.TypeOf(sql.NullString{})
	sqlStringType      = reflect.TypeOf("")
	sqlNullByteType    = reflect.TypeOf(sql.NullByte{})
	sqlByteType        = reflect.TypeOf('?')
	sqlIntType         = reflect.TypeOf(0)
	sqlInt8Type        = reflect.TypeOf(int8(0))
	sqlNullInt16Type   = reflect.TypeOf(sql.NullInt16{})
	sqlInt16Type       = reflect.TypeOf(int16(0))
	sqlNullInt32Type   = reflect.TypeOf(sql.NullInt32{})
	sqlInt32Type       = reflect.TypeOf(int32(0))
	sqlNullInt64Type   = reflect.TypeOf(sql.NullInt64{})
	sqlInt64Type       = reflect.TypeOf(int64(0))
	sqlNullFloat64Type = reflect.TypeOf(sql.NullFloat64{})
	sqlFloat64Type     = reflect.TypeOf(float64(0))
	sqlFloat32Type     = reflect.TypeOf(float32(0))
	sqlNullBoolType    = reflect.TypeOf(sql.NullBool{})
	sqlBoolType        = reflect.TypeOf(false)
	sqlNullTimeType    = reflect.TypeOf(sql.NullTime{})
	sqlTimeType        = reflect.TypeOf(time.Time{})
	sqlRawType         = reflect.TypeOf(sql.RawBytes{})
	sqlBytesType       = reflect.TypeOf([]byte{})
)

type ColumnType string

type Column struct {
	Type  ColumnType      `json:"type,omitempty"`
	Name  string          `json:"name,omitempty"`
	Value json.RawMessage `json:"value,omitempty"`
	Nil   bool            `json:"nil,omitempty"`
}

type Row struct {
	Columns []*Column `json:"columns,omitempty"`
}

func (r *Row) Scan(v interface{}) (err error) {
	if v == nil {
		err = fmt.Errorf("fns SQL Row: scan at a nil point value")
		return
	}
	typ := reflect.TypeOf(v)
	if typ.Kind() != reflect.Ptr {
		err = fmt.Errorf("fns SQL Row: target is not ptr")
		return
	}
	if typ.Elem().Kind() != reflect.Struct {
		err = fmt.Errorf("fns SQL Row: target elem is not struct")
		return
	}
	if r.Columns == nil || len(r.Columns) == 0 {
		return
	}

	ref := make(map[string]*Column)

	for i := 0; i < typ.NumField(); i++ {
		field := typ.Field(i)
		tagValue, hasTag := field.Tag.Lookup(columnStructTag)
		if !hasTag {
			continue
		}
		tagValue = strings.TrimSpace(tagValue)
		if tagValue == "" {
			continue
		}
		for _, column := range r.Columns {
			if column.Name == tagValue {
				ref[field.Name] = column
				break
			}
		}
	}

	if len(ref) == 0 {
		return
	}

	rv := reflect.ValueOf(v)
	for name, column := range ref {
		if column.Nil {
			continue
		}
		fv := rv.Elem().FieldByName(name)
		switch column.Type {
		case StringType:
			x := ""
			_ = json.Unmarshal(column.Value, &x)
			if fv.Type() == sqlNullStringType {
				v := sql.NullString{
					String: x,
					Valid:  true,
				}
				fv.Set(reflect.ValueOf(v))
			} else {
				fv.SetString(x)
			}
		case ByteType:
			x := byte('0')
			_ = json.Unmarshal(column.Value, &x)
			if fv.Type() == sqlNullByteType {
				v := sql.NullByte{
					Byte:  x,
					Valid: true,
				}
				fv.Set(reflect.ValueOf(v))
			} else {
				fv.Set(reflect.ValueOf(x))
			}
		case BytesType:
			x := make([]byte, 0, 1)
			_ = json.Unmarshal(column.Value, &x)
			fv.SetBytes(x)
		case IntType:
			x := int64(0)
			_ = json.Unmarshal(column.Value, &x)
			if fv.Type() == sqlNullInt16Type {
				v := sql.NullInt16{
					Int16: int16(x),
					Valid: true,
				}
				fv.Set(reflect.ValueOf(v))
			} else if fv.Type() == sqlNullInt32Type {
				v := sql.NullInt32{
					Int32: int32(x),
					Valid: true,
				}
				fv.Set(reflect.ValueOf(v))
			} else if fv.Type() == sqlNullInt64Type {
				v := sql.NullInt64{
					Int64: x,
					Valid: true,
				}
				fv.Set(reflect.ValueOf(v))
			} else {
				fv.SetInt(x)
			}
		case FloatType:
			x := float64(0)
			_ = json.Unmarshal(column.Value, &x)
			if fv.Type() == sqlNullFloat64Type {
				v := sql.NullFloat64{
					Float64: x,
					Valid:   true,
				}
				fv.Set(reflect.ValueOf(v))
			} else {
				fv.SetFloat(x)
			}
		case TimeType:
			x := time.Time{}
			_ = json.Unmarshal(column.Value, &x)
			if fv.Type() == sqlNullFloat64Type {
				v := sql.NullTime{
					Time:  x,
					Valid: true,
				}
				fv.Set(reflect.ValueOf(v))
			} else {
				fv.Set(reflect.ValueOf(x))
			}
		case BoolType:
			x := false
			_ = json.Unmarshal(column.Value, &x)
			if fv.Type() == sqlNullFloat64Type {
				v := sql.NullBool{
					Bool:  x,
					Valid: true,
				}
				fv.Set(reflect.ValueOf(v))
			} else {
				fv.SetBool(x)
			}
		}
	}

	return
}
