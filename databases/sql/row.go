package sql

import (
	db "database/sql"
	stdJson "encoding/json"
	"fmt"
	"github.com/aacfactory/json"
	"reflect"
	"strings"
	"time"
	"unsafe"
)

func NewRows(raws *db.Rows) (r *Rows, err error) {

	colTypes, colTypesErr := raws.ColumnTypes()
	if colTypesErr != nil {
		err = colTypesErr
		return
	}

	rows := make([]*Row, 0, 1)
	for raws.Next() {
		columns := make([]interface{}, 0, 1)

		for _, colType := range colTypes {
			column := NewColumnScanner(colType)
			columns = append(columns, column)
		}

		scanErr := raws.Scan(columns...)
		if scanErr != nil {
			err = scanErr
			return
		}

		rows = append(rows, &Row{
			columns: reflect.NewAt(reflect.SliceOf(reflect.TypeOf(&Column{})), unsafe.Pointer(reflect.ValueOf(&columns).Pointer())).Elem().Interface().([]*Column),
		})

	}

	r = &Rows{
		values: rows,
	}
	return
}

type Rows struct {
	idx    int
	values []*Row
}

func (r *Rows) MarshalJSON() (p []byte, err error) {
	if r.Empty() {
		p = []byte{'[', ']'}
		return
	}
	p, err = json.Marshal(r.values)
	return
}

func (r *Rows) UnmarshalJSON(p []byte) (err error) {
	r.values = make([]*Row, 0, 1)
	if p == nil || len(p) == 0 {
		return
	}
	err = json.Unmarshal(p, &r.values)
	return
}

func (r *Rows) Empty() (ok bool) {
	ok = r.values == nil || len(r.values) == 0
	return
}

func (r *Rows) Size() int {
	if r.Empty() {
		return 0
	}
	return len(r.values)
}

func (r *Rows) Next() (v *Row, has bool) {
	if r.Empty() {
		return
	}
	has = r.idx < r.Size()
	if has {
		v = r.values[r.idx]
		r.idx++
	}
	return
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
		for _, value := range r.values {
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
		err = r.values[0].Scan(v)
	} else if typ.Elem().Kind() == reflect.Interface {
		rv := reflect.Indirect(reflect.ValueOf(v))
		if reflect.TypeOf(rv.Interface()).Kind() != reflect.Slice {
			err = fmt.Errorf("fns SQL Rows: scan failed for target elem is not slice or struct")
			return
		}
		var elemType reflect.Type
		elemIsPtr := false
		rvt := reflect.TypeOf(rv.Interface())
		elem := rvt.Elem()
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
		rv0 := reflect.MakeSlice(rvt, 0, 1)
		for _, value := range r.values {
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
	columns []*Column
}

func (r *Row) MarshalJSON() (p []byte, err error) {
	if r.Empty() {
		p = []byte{'[', ']'}
		return
	}
	p, err = json.Marshal(r.columns)
	return
}

func (r *Row) UnmarshalJSON(p []byte) (err error) {
	r.columns = make([]*Column, 0, 1)
	if p == nil || len(p) == 0 {
		return
	}
	err = json.Unmarshal(p, &r.columns)
	return
}

func (r *Row) Empty() (ok bool) {
	ok = r.columns == nil || len(r.columns) == 0
	return
}

func (r *Row) Columns() (columns []*Column) {
	columns = r.columns
	return
}

func (r *Row) Column(name string, value interface{}) (has bool, err error) {
	if r.Empty() {
		return
	}
	for _, column := range r.columns {
		if column.Name == name {
			has = true
			err = column.Decode(value)
			return
		}
	}
	return
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
	if r.columns == nil || len(r.columns) == 0 {
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

		for _, column := range r.columns {
			if strings.ToLower(column.Name) == strings.ToLower(colName) {
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
				vv := reflect.New(fv.Type().Elem())
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
		if kind == columnTagPk {
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
