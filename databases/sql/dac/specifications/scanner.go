package specifications

import (
	"bytes"
	stdsql "database/sql"
	"fmt"
	"github.com/aacfactory/errors"
	"github.com/aacfactory/fns-contrib/databases/sql"
	"github.com/aacfactory/fns/commons/times"
	"github.com/aacfactory/json"
	"reflect"
	"sync"
	"time"
	"unsafe"
)

type ScanValue interface {
	Scan(src any) (err error)
	Value() (v any, valid bool)
}

type DateValue struct {
	valid bool
	value times.Date
}

func (v *DateValue) Scan(src any) (err error) {
	switch ss := src.(type) {
	case string:
		t, parseErr := time.Parse("2006-01-02", ss)
		if parseErr != nil {
			err = parseErr
			return
		}
		v.value = times.DataOf(t)
		v.valid = true
		break
	case []byte:
		t, parseErr := time.Parse("2006-01-02", unsafe.String(unsafe.SliceData(ss), len(ss)))
		if parseErr != nil {
			err = parseErr
			return
		}
		v.value = times.DataOf(t)
		v.valid = true
		break
	case time.Time:
		v.value = times.DataOf(ss)
		v.valid = true
		break
	case nil:
		break
	default:
		sv := stdsql.NullTime{}
		err = sv.Scan(src)
		if err != nil {
			return
		}
		if sv.Valid {
			v.value = times.DataOf(sv.Time)
			v.valid = true
		}
		break
	}
	return
}

func (v *DateValue) Value() (value any, valid bool) {
	valid = v.valid
	if valid {
		value = v.value
	}
	return
}

type TimeValue struct {
	valid bool
	value times.Time
}

func (v *TimeValue) Scan(src any) (err error) {
	switch ss := src.(type) {
	case string:
		t, parseErr := time.Parse("15:04:05", ss)
		if parseErr != nil {
			err = parseErr
			return
		}
		v.value = times.TimeOf(t)
		v.valid = true
		break
	case []byte:
		t, parseErr := time.Parse("15:04:05", unsafe.String(unsafe.SliceData(ss), len(ss)))
		if parseErr != nil {
			err = parseErr
			return
		}
		v.value = times.TimeOf(t)
		v.valid = true
		break
	case time.Time:
		v.value = times.TimeOf(ss)
		v.valid = true
		break
	case nil:
		break
	default:
		sv := stdsql.NullTime{}
		err = sv.Scan(src)
		if err != nil {
			return
		}
		if sv.Valid {
			v.value = times.TimeOf(sv.Time)
			v.valid = true
		}
		break
	}
	return
}

func (v *TimeValue) Value() (value any, valid bool) {
	valid = v.valid
	if valid {
		value = v.value
	}
	return
}

type JsonValue struct {
	valid bool
	value json.RawMessage
}

func (v *JsonValue) Scan(src any) (err error) {
	if v.value == nil {
		v.value = json.RawMessage{}
	}
	err = v.value.Scan(src)
	if err != nil {
		return
	}
	v.valid = v.value.Valid()
	return
}

func (v *JsonValue) Value() (value any, valid bool) {
	valid = v.valid
	if valid {
		value = v.value
	}
	return
}

var (
	genericsPool = sync.Pool{}
)

func acquireGenerics(n int) (v Generics) {
	c := genericsPool.Get()
	if c == nil {
		v = make(Generics, n)
		for i := 0; i < n; i++ {
			v[i] = &Generic{}
		}
	} else {
		v = c.(Generics)
		vLen := len(v)
		if delta := n - vLen; delta < 0 {
			v = v[0:n]
		} else if delta > 0 {
			for i := 0; i < delta; i++ {
				v = append(v, &Generic{})
			}
		}
	}
	return
}

func releaseGenerics(vv ...Generics) {
	for _, generics := range vv {
		for _, generic := range generics {
			generic.(*Generic).Reset()
		}
		genericsPool.Put(generics)
	}
}

type Generics []any

func (generics Generics) WriteTo(spec *Specification, fieldNames []string, entryPtr any) (err error) {
	rv := reflect.Indirect(reflect.ValueOf(entryPtr))
	for i, fieldName := range fieldNames {
		column, has := spec.ColumnByField(fieldName)
		if !has {
			err = errors.Warning(fmt.Sprintf("sql: %s field was not found in %s", fieldName, spec.Key)).
				WithMeta("field", fieldName).WithMeta("table", spec.Key)
			return
		}
		fv := rv.FieldByName(fieldName)
		generic := generics[i].(*Generic)
		err = generic.WriteTo(column, fv)
		if err != nil {
			err = errors.Warning(fmt.Sprintf("sql: write value into %s %s field failed", spec.Key, fieldName)).WithCause(err).
				WithMeta("field", fieldName).WithMeta("table", spec.Key)
			return
		}
	}
	return
}

type Generic struct {
	Valid bool
	Value any
}

func (v *Generic) Scan(src any) (err error) {
	if src == nil {
		return
	}
	v.Valid = true
	v.Value = src
	return
}

func (v *Generic) Reset() {
	v.Valid = false
	v.Value = nil
}

func (v *Generic) WriteTo(column *Column, rv reflect.Value) (err error) {
	if !v.Valid {
		return
	}
	srv := reflect.ValueOf(v.Value)
	if srv.Type() == rv.Type() {
		rv.Set(srv)
		return
	}
	switch column.Type.Name {
	case StringType:
		vv, ok := v.Value.(string)
		if ok {
			if rv.Type().Kind() == reflect.String {
				rv.SetString(vv)
			} else if rv.Type().ConvertibleTo(nullStringType) {
				rv.Set(reflect.ValueOf(stdsql.NullString{
					String: vv,
					Valid:  vv != "",
				}).Convert(rv.Type()))
			} else {
				err = errors.Warning("sql: scan rows failed").
					WithCause(fmt.Errorf("field is not string")).
					WithMeta("name", column.Name)
				return
			}
			break
		}
		err = errors.Warning("sql: scan rows failed").
			WithCause(fmt.Errorf("value is not string")).
			WithMeta("name", column.Name)
		return
	case BoolType:
		vv, ok := v.Value.(bool)
		if ok {
			if rv.Type().Kind() == reflect.Bool {
				rv.SetBool(vv)
			} else if rv.Type().ConvertibleTo(nullStringType) {
				rv.Set(reflect.ValueOf(stdsql.NullBool{
					Bool:  vv,
					Valid: true,
				}).Convert(rv.Type()))
			} else {
				err = errors.Warning("sql: scan rows failed").
					WithCause(fmt.Errorf("field is not bool")).
					WithMeta("name", column.Name)
				return
			}
			break
		}
		err = errors.Warning("sql: scan rows failed").
			WithCause(fmt.Errorf("value is not bool")).
			WithMeta("name", column.Name)
		return
	case IntType:
		n, ok := AsInt(v.Value)
		if ok {
			switch rv.Type().Kind() {
			case reflect.Int64, reflect.Int, reflect.Int32, reflect.Int16, reflect.Int8:
				rv.SetInt(n)
				break
			default:
				if rv.Type().ConvertibleTo(nullInt64Type) {
					rv.Set(reflect.ValueOf(stdsql.NullInt64{
						Int64: n,
						Valid: true,
					}).Convert(rv.Type()))
				} else if rv.Type().ConvertibleTo(nullInt32Type) {
					rv.Set(reflect.ValueOf(stdsql.NullInt32{
						Int32: int32(n),
						Valid: true,
					}).Convert(rv.Type()))
				} else if rv.Type().ConvertibleTo(nullInt16Type) {
					rv.Set(reflect.ValueOf(stdsql.NullInt16{
						Int16: int16(n),
						Valid: true,
					}).Convert(rv.Type()))
				} else {
					err = errors.Warning("sql: scan rows failed").
						WithCause(fmt.Errorf("field is not int")).
						WithMeta("name", column.Name)
					return
				}
			}
			break
		}
		err = errors.Warning("sql: scan rows failed").
			WithCause(fmt.Errorf("value is not int")).
			WithMeta("name", column.Name)
		return
	case FloatType:
		f, ok := AsFloat(v.Value)
		if ok {
			switch rv.Type().Kind() {
			case reflect.Float64, reflect.Float32:
				rv.SetFloat(f)
				break
			default:
				if rv.Type().ConvertibleTo(nullFloatType) {
					rv.Set(reflect.ValueOf(stdsql.NullFloat64{
						Float64: f,
						Valid:   true,
					}).Convert(rv.Type()))
				} else {
					err = errors.Warning("sql: scan rows failed").
						WithCause(fmt.Errorf("field is not float")).
						WithMeta("name", column.Name)
					return
				}
			}
			break
		}
		err = errors.Warning("sql: scan rows failed").
			WithCause(fmt.Errorf("value is not float")).
			WithMeta("name", column.Name)
		return
	case ByteType:
		b, ok := v.Value.(byte)
		if ok {
			switch rv.Type().Kind() {
			case reflect.Uint8:
				rv.Set(reflect.ValueOf(b))
				break
			default:
				if rv.Type().ConvertibleTo(nullByteType) {
					rv.Set(reflect.ValueOf(stdsql.NullByte{
						Byte:  b,
						Valid: true,
					}).Convert(rv.Type()))
				} else {
					err = errors.Warning("sql: scan rows failed").
						WithCause(fmt.Errorf("field is not byte")).
						WithMeta("name", column.Name)
					return
				}
			}
			break
		}
		err = errors.Warning("sql: scan rows failed").
			WithCause(fmt.Errorf("value is not byte")).
			WithMeta("name", column.Name)
		return
	case BytesType:
		p, ok := v.Value.([]byte)
		if ok {
			if rv.Type().ConvertibleTo(bytesType) {
				rv.Set(reflect.ValueOf(p).Convert(rv.Type()))
			} else if rv.Type().ConvertibleTo(nullBytesType) {
				rv.Set(reflect.ValueOf(sql.NullBytes{
					Bytes: p,
					Valid: true,
				}).Convert(rv.Type()))
			} else {
				err = errors.Warning("sql: scan rows failed").
					WithCause(fmt.Errorf("field is not bytes")).
					WithMeta("name", column.Name)
				return
			}
			break
		}
		err = errors.Warning("sql: scan rows failed").
			WithCause(fmt.Errorf("value is not bytes")).
			WithMeta("name", column.Name)
		return
	case DatetimeType:
		t, ok := v.Value.(time.Time)
		if ok {
			if rv.Type().ConvertibleTo(datetimeType) {
				rv.Set(reflect.ValueOf(t).Convert(rv.Type()))
			} else if rv.Type().ConvertibleTo(nullTimeType) {
				rv.Set(reflect.ValueOf(stdsql.NullTime{
					Time:  t,
					Valid: !t.IsZero(),
				}).Convert(rv.Type()))
			} else {
				err = errors.Warning("sql: scan rows failed").
					WithCause(fmt.Errorf("field is not time.Time")).
					WithMeta("name", column.Name)
				return
			}
			break
		}
		err = errors.Warning("sql: scan rows failed").
			WithCause(fmt.Errorf("value is not time.Time")).
			WithMeta("name", column.Name)
		return
	case DateType:
		t, ok := v.Value.(time.Time)
		if ok {
			rv.Set(reflect.ValueOf(times.DataOf(t)))
			break
		}
		err = errors.Warning("sql: scan rows failed").
			WithCause(fmt.Errorf("value is not time.Time")).
			WithMeta("name", column.Name)
		return
	case TimeType:
		t, ok := v.Value.(time.Time)
		if ok {
			rv.Set(reflect.ValueOf(times.TimeOf(t)))
			break
		}
		err = errors.Warning("sql: scan rows failed").
			WithCause(fmt.Errorf("value is not time.Time")).
			WithMeta("name", column.Name)
		return
	case JsonType, MappingType:
		p, ok := v.Value.([]byte)
		if ok {
			if json.IsNull(p) || bytes.Equal(p, jsonEmptyBytes) || bytes.Equal(p, jsonEmptyArrayBytes) {
				break
			}
			if !json.Validate(p) {
				err = errors.Warning("sql: scan rows failed").
					WithCause(fmt.Errorf("value is not valid json bytes")).
					WithMeta("name", column.Name)
				return
			}
			if column.Type.Value.ConvertibleTo(bytesType) {
				rv.SetBytes(p)
				break
			}
			if column.Type.Value.Kind() == reflect.Ptr {
				rv.Set(reflect.New(rv.Type().Elem()))
				decodeErr := json.Unmarshal(p, rv.Interface())
				if decodeErr != nil {
					err = errors.Warning("sql: scan rows failed").
						WithCause(fmt.Errorf("value is not valid json bytes")).WithCause(decodeErr).
						WithMeta("name", column.Name)
					return
				}
			} else {
				element := reflect.New(rv.Type()).Interface()
				decodeErr := json.Unmarshal(p, element)
				if decodeErr != nil {
					err = errors.Warning("sql: scan rows failed").
						WithCause(fmt.Errorf("value is not valid json bytes")).WithCause(decodeErr).
						WithMeta("name", column.Name)
					return
				}
				rv.Set(reflect.ValueOf(element).Elem())
			}
			break
		}
		err = errors.Warning("sql: scan rows failed").
			WithCause(fmt.Errorf("value is not bytes")).
			WithMeta("name", column.Name)
		return
	case ScanType:
		scanner, ok := column.PtrValue().(stdsql.Scanner)
		if !ok {
			err = errors.Warning("sql: scan rows failed").
				WithCause(fmt.Errorf("field is not sql.Scanner")).
				WithMeta("name", column.Name)
			return
		}
		scanFieldValueErr := scanner.Scan(v.Value)
		if scanFieldValueErr != nil {
			err = errors.Warning("sql: scan rows failed").
				WithCause(fmt.Errorf("scan field value failed")).WithCause(scanFieldValueErr).
				WithMeta("name", column.Name)
			return
		}
		rv.Set(reflect.ValueOf(scanner))
		break
	default:
		err = errors.Warning("sql: scan rows failed").
			WithCause(fmt.Errorf("type of field is invalid")).
			WithMeta("name", column.Name)
		return
	}
	return
}
