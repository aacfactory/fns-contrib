package sql

import (
	db "database/sql"
	stdJson "encoding/json"
	"fmt"
	"github.com/aacfactory/json"
	"reflect"
	"strings"
	"time"
)

func NewRows(raws *db.Rows) (r *Rows, err error) {

	colTypes, colTypesErr := raws.ColumnTypes()
	if colTypesErr != nil {
		err = colTypesErr
		return
	}

	rows := make([]*Row, 0, 1)
	for raws.Next() {
		columns0 := make([]interface{}, 0, 1)
		columns := make([]*Column, 0, 1)

		for _, colType := range colTypes {
			column := createColumnValueByColumnType(colType)
			columns0 = append(columns0, column)
			columns = append(columns, column)
		}

		scanErr := raws.Scan(columns0...)
		if scanErr != nil {
			err = scanErr
			return
		}

		rows = append(rows, &Row{
			Columns: columns,
		})

	}

	r = &Rows{
		Values: rows,
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
		err = fmt.Errorf("fns SQL Rows: scan failed for target is not ptr")
		return
	}
	if r.Empty() {
		return
	}
	if typ.Elem().Kind() == reflect.Slice {
		var elemType reflect.Type
		elemIsPtr := false
		elem := typ.Elem().Elem()
		if elem.Kind() == reflect.Ptr {
			if elem.Elem().Kind() != reflect.Struct {
				err = fmt.Errorf("fns SQL Rows: scan failed for element of target is not struct or ptr of struct")
				return
			}
			elemIsPtr = true
			elemType = elem.Elem()
		} else if elem.Kind() == reflect.Struct {
			elemIsPtr = false
			elemType = elem
		} else {
			err = fmt.Errorf("fns SQL Rows: scan failed for element of target is not struct or ptr of struct")
			return
		}
		rv := reflect.ValueOf(v).Elem()
		rv0 := reflect.ValueOf(v).Elem()
		for _, value := range r.Values {
			x := reflect.New(elemType)
			err = value.Scan(x.Interface())
			if err != nil {
				return
			}
			if elemIsPtr {
				rv0 = reflect.Append(rv0, x)
			} else {
				rv0 = reflect.Append(rv0, x.Elem())
			}
		}
		rv.Set(rv0)
	} else if typ.Elem().Kind() == reflect.Struct {
		if r.Size() != 1 {
			err = fmt.Errorf("fns SQL Rows: scan failed for target elem is struct but has many rows")
			return
		}
		err = r.Values[0].Scan(v)
	} else {
		err = fmt.Errorf("fns SQL Rows: scan failed for target elem is not slice or struct")
		return
	}
	return
}

var (
	sqlNullStringType  = reflect.TypeOf(db.NullString{})
	sqlStringType      = reflect.TypeOf("")
	sqlIntType         = reflect.TypeOf(0)
	sqlInt8Type        = reflect.TypeOf(int8(0))
	sqlNullInt16Type   = reflect.TypeOf(db.NullInt16{})
	sqlInt16Type       = reflect.TypeOf(int16(0))
	sqlNullInt32Type   = reflect.TypeOf(db.NullInt32{})
	sqlInt32Type       = reflect.TypeOf(int32(0))
	sqlNullInt64Type   = reflect.TypeOf(db.NullInt64{})
	sqlInt64Type       = reflect.TypeOf(int64(0))
	sqlNullFloat64Type = reflect.TypeOf(db.NullFloat64{})
	sqlFloat64Type     = reflect.TypeOf(float64(0))
	sqlFloat32Type     = reflect.TypeOf(float32(0))
	sqlNullBoolType    = reflect.TypeOf(db.NullBool{})
	sqlBoolType        = reflect.TypeOf(false)
	sqlNullTimeType    = reflect.TypeOf(db.NullTime{})
	sqlTimeType        = reflect.TypeOf(time.Time{})
	sqlBytesType       = reflect.TypeOf([]byte{})
	sqlJsonType        = reflect.TypeOf(json.RawMessage{})
	sqlSTDJsonType     = reflect.TypeOf(stdJson.RawMessage{})
)

type Row struct {
	Columns []*Column `json:"columns,omitempty"`
}

func (r *Row) Scan(target interface{}) (err error) {
	if target == nil {
		err = fmt.Errorf("fns SQL Row: scan at a nil point value")
		return
	}
	typ := reflect.TypeOf(target)
	if typ.Kind() != reflect.Ptr {
		err = fmt.Errorf("fns SQL Row: scan failed for target is not ptr")
		return
	}
	if typ.Elem().Kind() != reflect.Struct {
		err = fmt.Errorf("fns SQL Row: scan failed for target elem is not struct")
		return
	}
	if r.Columns == nil || len(r.Columns) == 0 {
		return
	}

	ref := make(map[string]*FieldColumn)

	for i := 0; i < typ.Elem().NumField(); i++ {
		field := typ.Elem().Field(i)
		tagValue, hasTag := field.Tag.Lookup(columnStructTag)
		if !hasTag {
			continue
		}
		tagValue = strings.ToUpper(strings.TrimSpace(tagValue))
		if tagValue == "" {
			continue
		}

		colName := ""
		colKind := ""
		if strings.Contains(tagValue, ",") {
			tagValues := strings.Split(tagValue, ",")
			colName = strings.TrimSpace(tagValues[0])
			colKind = strings.TrimSpace(tagValues[1])
		} else {
			colName = tagValue
		}
		if colName == "-" {
			continue
		}

		for _, column := range r.Columns {
			if column.Name == colName {
				ref[field.Name] = &FieldColumn{
					Kind:      colKind,
					FieldType: field.Type,
					Column:    column,
				}
				break
			}
		}
	}

	if len(ref) == 0 {
		return
	}

	rv := reflect.ValueOf(target)
	for name, fieldColumn := range ref {
		column := fieldColumn.Column
		if column.Nil {
			continue
		}
		fv := rv.Elem().FieldByName(name)

		switch column.Type {
		case StringType:
			x := ""
			_ = json.Unmarshal(column.Value, &x)
			if fv.Type() == sqlNullStringType {
				v := db.NullString{
					String: x,
					Valid:  true,
				}
				fv.Set(reflect.ValueOf(v))
			} else if fv.Type() == sqlStringType {
				fv.SetString(x)
			} else if fv.Type().Kind() == reflect.Ptr {
				vv := reflect.New(fv.Type())
				rowScanFK(fieldColumn, vv, x)
				fv.Set(vv)
			}
		case BytesType:
			x := make([]byte, 0, 1)
			_ = json.Unmarshal(column.Value, &x)
			fv.SetBytes(x)
		case IntType:
			x := int64(0)
			_ = json.Unmarshal(column.Value, &x)
			if fv.Type() == sqlNullInt16Type {
				v := db.NullInt16{
					Int16: int16(x),
					Valid: true,
				}
				fv.Set(reflect.ValueOf(v))
			} else if fv.Type() == sqlNullInt32Type {
				v := db.NullInt32{
					Int32: int32(x),
					Valid: true,
				}
				fv.Set(reflect.ValueOf(v))
			} else if fv.Type() == sqlNullInt64Type {
				v := db.NullInt64{
					Int64: x,
					Valid: true,
				}
				fv.Set(reflect.ValueOf(v))
			} else if fv.Type() == sqlIntType || fv.Type() == sqlInt8Type || fv.Type() == sqlInt16Type || fv.Type() == sqlInt32Type || fv.Type() == sqlInt64Type {
				fv.SetInt(x)
			} else if fv.Type().Kind() == reflect.Ptr {
				vv := reflect.New(fv.Type().Elem())
				rowScanFK(fieldColumn, vv, x)
				fv.Set(vv)
			}
		case FloatType:
			x := float64(0)
			_ = json.Unmarshal(column.Value, &x)
			if fv.Type() == sqlNullFloat64Type {
				v := db.NullFloat64{
					Float64: x,
					Valid:   true,
				}
				fv.Set(reflect.ValueOf(v))
			} else if fv.Type() == sqlFloat32Type || fv.Type() == sqlFloat64Type {
				fv.SetFloat(x)
			}
		case TimeType:
			x := time.Time{}
			_ = json.Unmarshal(column.Value, &x)
			if fv.Type() == sqlNullTimeType {
				v := db.NullTime{
					Time:  x,
					Valid: true,
				}
				fv.Set(reflect.ValueOf(v))
			} else if fv.Type() == sqlTimeType {
				fv.Set(reflect.ValueOf(x))
			}
		case BoolType:
			x := false
			_ = json.Unmarshal(column.Value, &x)
			if fv.Type() == sqlNullBoolType {
				v := db.NullBool{
					Bool:  x,
					Valid: true,
				}
				fv.Set(reflect.ValueOf(v))
			} else if fv.Type() == sqlBoolType {
				fv.SetBool(x)
			}
		case JsonType:
			x := column.Value
			if fv.Type() == sqlJsonType || fv.Type() == sqlSTDJsonType {
				fv.SetBytes(x)
			} else if fv.Type().Kind() == reflect.Ptr || fv.Type().Kind() == reflect.Slice {
				vv := reflect.New(fv.Type()).Interface()
				decodeErr := json.Unmarshal(x, vv)
				if decodeErr != nil {
					err = fmt.Errorf("fns SQL Row: scan failed for decode json of %v is not supported", name)
					return
				}
				fv.Set(reflect.ValueOf(vv).Elem())
			}
		case UnknownType:
			if fv.Type().AssignableTo(sqlBytesType) {
				fv.SetBytes(column.Value)
			}
		default:
			err = fmt.Errorf("fns SQL Row: scan failed for %s of %s is not supported", fv.Type().String(), name)
		}
	}
	return
}

func rowScanFK(fc *FieldColumn, fv reflect.Value, v interface{}) {
	fvType := fv.Elem().Type()
	pkFieldName := ""
	for i := 0; i < fvType.NumField(); i++ {
		field := fvType.Field(i)
		tagValue, hasTag := field.Tag.Lookup(columnStructTag)
		if !hasTag {
			continue
		}
		tagValue = strings.ToUpper(strings.TrimSpace(tagValue))
		if tagValue == "" || tagValue == "-" || !strings.Contains(tagValue, ",") {
			continue
		}
		kind := tagValue[strings.Index(tagValue, ",")+1:]
		if kind == associationKindFK {
			pkFieldName = field.Name
			break
		}
	}
	if pkFieldName == "" {
		return
	}
	switch fc.Column.Type {
	case StringType:
		sv, ok := v.(string)
		if ok {
			fv.Elem().FieldByName(pkFieldName).SetString(sv)
		}
	case IntType:
		sv, ok := v.(int64)
		if ok {
			fv.Elem().FieldByName(pkFieldName).SetInt(sv)
		}
	}
}
