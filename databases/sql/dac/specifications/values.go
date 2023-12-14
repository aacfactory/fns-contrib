package specifications

import (
	"bytes"
	"context"
	stdsql "database/sql"
	"fmt"
	"github.com/aacfactory/errors"
	"github.com/aacfactory/fns-contrib/databases/sql"
	"github.com/aacfactory/fns/commons/times"
	"github.com/aacfactory/json"
	"reflect"
	"time"
)

func ScanRows[T any](ctx context.Context, rows sql.Rows, fields []string) (entries []T, err error) {
	spec, specErr := GetSpecification(ctx, Instance[T]())
	if specErr != nil {
		err = specErr
		return
	}
	for rows.Next() {
		generics := acquireGenerics(len(fields))
		scanErr := rows.Scan(generics...)
		if scanErr != nil {
			releaseGenerics(generics)
			err = scanErr
			return
		}
		entry := Instance[T]()
		writeErr := generics.WriteTo(spec, fields, &entry)
		releaseGenerics(generics)
		if writeErr != nil {
			err = scanErr
			return
		}
		entries = append(entries, entry)
	}
	return
}

func NewBasicValueWriter(rt reflect.Type) (vw ValueWriter, ct ColumnTypeName, err error) {
	switch rt.Kind() {
	case reflect.String:
		vw = &StringValue{}
		ct = StringType
		break
	case reflect.Bool:
		vw = &BoolValue{}
		ct = BoolType
		break
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		vw = &IntValue{}
		ct = IntType
		break
	case reflect.Float32, reflect.Float64:
		vw = &FloatValue{}
		ct = FloatType
		break
	case reflect.Uint8:
		vw = &ByteValue{}
		ct = ByteType
		break
	default:
		if rt.ConvertibleTo(nullStringType) {
			vw = &StringValue{null: true}
			ct = StringType
			break
		}
		if rt.ConvertibleTo(nullBoolType) {
			vw = &BoolValue{null: true}
			ct = BoolType
			break
		}
		if rt.ConvertibleTo(nullInt64Type) {
			vw = &IntValue{null: true, base: 64}
			ct = IntType
			break
		}
		if rt.ConvertibleTo(nullInt32Type) {
			vw = &IntValue{null: true, base: 32}
			ct = IntType
			break
		}
		if rt.ConvertibleTo(nullInt16Type) {
			vw = &IntValue{null: true, base: 16}
			ct = IntType
			break
		}
		if rt.ConvertibleTo(nullFloatType) {
			vw = &FloatValue{null: true}
			ct = FloatType
			break
		}
		if rt.ConvertibleTo(nullByteType) {
			vw = &ByteValue{null: true}
			ct = ByteType
			break
		}
		if rt.ConvertibleTo(nullBytesType) {
			vw = &BytesValue{null: true}
			ct = BytesType
			break
		}
		if rt.ConvertibleTo(datetimeType) {
			vw = &DatetimeValue{}
			ct = DatetimeType
			break
		}
		if rt.ConvertibleTo(nullTimeType) || rt.ConvertibleTo(nullDatetimeType) {
			vw = &DatetimeValue{null: true}
			ct = DatetimeType
			break
		}
		if rt.ConvertibleTo(dateType) {
			vw = &DateValue{}
			ct = DateType
			break
		}
		if rt.ConvertibleTo(nullTimesDateType) {
			vw = &DateValue{
				null: true,
			}
			ct = DateType
			break
		}
		if rt.ConvertibleTo(timeType) {
			vw = &TimeValue{}
			ct = TimeType
			break
		}
		if rt.ConvertibleTo(nullTimesTimeType) {
			vw = &TimeValue{
				null: true,
			}
			ct = TimeType
			break
		}
		err = errors.Warning("sql: type is not basic").WithCause(fmt.Errorf("%s", rt.String()))
		break
	}
	return
}

type ValueWriter interface {
	Write(value any, field reflect.Value) (err error)
}

type StringValue struct {
	null bool
}

func (w *StringValue) Write(value any, rv reflect.Value) (err error) {
	vv, ok := value.(string)
	if ok {
		if w.null {
			rv.Set(reflect.ValueOf(stdsql.NullString{
				String: vv,
				Valid:  vv != "",
			}).Convert(rv.Type()))
		} else {
			rv.SetString(vv)
		}
		return
	}
	err = errors.Warning("sql: write value failed").
		WithCause(fmt.Errorf("rv is not string"))
	return
}

type BoolValue struct {
	null bool
}

func (w *BoolValue) Write(value any, rv reflect.Value) (err error) {
	vv, ok := value.(bool)
	if ok {
		if w.null {
			rv.Set(reflect.ValueOf(stdsql.NullBool{
				Bool:  vv,
				Valid: true,
			}).Convert(rv.Type()))
		} else {
			rv.SetBool(vv)
		}
		return
	}
	err = errors.Warning("sql: write value failed").
		WithCause(fmt.Errorf("rv is not bool"))
	return
}

type IntValue struct {
	null bool
	base int
}

func (w *IntValue) Write(value any, rv reflect.Value) (err error) {
	n, ok := AsInt(value)
	if ok {
		if w.null {
			switch w.base {
			case 64:
				rv.Set(reflect.ValueOf(stdsql.NullInt64{
					Int64: n,
					Valid: true,
				}).Convert(rv.Type()))
				break
			case 32:
				rv.Set(reflect.ValueOf(stdsql.NullInt32{
					Int32: int32(n),
					Valid: true,
				}).Convert(rv.Type()))
				break
			default:
				rv.Set(reflect.ValueOf(stdsql.NullInt16{
					Int16: int16(n),
					Valid: true,
				}).Convert(rv.Type()))
				break
			}
		} else {
			rv.SetInt(n)
		}
		return
	}
	err = errors.Warning("sql: write value failed").
		WithCause(fmt.Errorf("rv is not int"))
	return
}

type FloatValue struct {
	null bool
}

func (w *FloatValue) Write(value any, rv reflect.Value) (err error) {
	f, ok := AsFloat(value)
	if ok {
		if w.null {
			rv.Set(reflect.ValueOf(stdsql.NullFloat64{
				Float64: f,
				Valid:   true,
			}).Convert(rv.Type()))
		} else {
			rv.SetFloat(f)
		}
		return
	}
	err = errors.Warning("sql: write value failed").
		WithCause(fmt.Errorf("rv is not float"))
	return
}

type ByteValue struct {
	null bool
}

func (w *ByteValue) Write(value any, rv reflect.Value) (err error) {
	b, ok := value.(byte)
	if ok {
		if w.null {
			rv.Set(reflect.ValueOf(stdsql.NullByte{
				Byte:  b,
				Valid: true,
			}).Convert(rv.Type()))
		} else {
			rv.Set(reflect.ValueOf(b))
		}
		return
	}
	err = errors.Warning("sql: write value failed").
		WithCause(fmt.Errorf("rv is not byte"))
	return
}

type BytesValue struct {
	null bool
}

func (w *BytesValue) Write(value any, rv reflect.Value) (err error) {
	p, ok := value.([]byte)
	if ok {
		if w.null {
			rv.Set(reflect.ValueOf(sql.NullBytes{
				Bytes: p,
				Valid: true,
			}).Convert(rv.Type()))
		} else {
			rv.Set(reflect.ValueOf(p).Convert(rv.Type()))
		}
		return
	}
	err = errors.Warning("sql: write value failed").
		WithCause(fmt.Errorf("rv is not bytes"))
	return
}

type DatetimeValue struct {
	null bool
}

func (w *DatetimeValue) Write(value any, rv reflect.Value) (err error) {
	t, ok := value.(time.Time)
	if ok {
		if w.null {
			if rv.Type().ConvertibleTo(nullTimeType) {
				rv.Set(reflect.ValueOf(stdsql.NullTime{
					Time:  t,
					Valid: !t.IsZero(),
				}).Convert(rv.Type()))
			} else if rv.Type().ConvertibleTo(nullDatetimeType) {
				rv.Set(reflect.ValueOf(sql.NullDatetime{
					NullTime: stdsql.NullTime{
						Time:  t,
						Valid: !t.IsZero(),
					},
				}).Convert(rv.Type()))
			}
		} else {
			rv.Set(reflect.ValueOf(t).Convert(rv.Type()))
		}
		return
	}
	err = errors.Warning("sql: write value failed").
		WithCause(fmt.Errorf("rv is not time.Time"))
	return
}

type DateValue struct {
	null bool
}

func (w *DateValue) Write(value any, rv reflect.Value) (err error) {
	t, ok := value.(time.Time)
	if ok {
		if w.null {
			rv.Set(reflect.ValueOf(sql.NullDate{
				Date:  times.DataOf(t),
				Valid: !t.IsZero(),
			}).Convert(rv.Type()))
		} else {
			rv.Set(reflect.ValueOf(times.DataOf(t)))
		}
		return
	}
	err = errors.Warning("sql: write value failed").
		WithCause(fmt.Errorf("rv is not time.Time"))
	return
}

type TimeValue struct {
	null bool
}

func (w *TimeValue) Write(value any, rv reflect.Value) (err error) {
	t, ok := value.(time.Time)
	if ok {
		if w.null {
			rv.Set(reflect.ValueOf(sql.NullTime{
				Time:  times.TimeOf(t),
				Valid: !t.IsZero(),
			}).Convert(rv.Type()))
		} else {
			rv.Set(reflect.ValueOf(times.TimeOf(t)))
		}
		return
	}
	err = errors.Warning("sql: write value failed").
		WithCause(fmt.Errorf("rv is not time.Time"))
	return
}

type JsonValue struct {
	ValueType reflect.Type
}

func (w *JsonValue) Write(value any, rv reflect.Value) (err error) {
	p, ok := value.([]byte)
	if ok {
		if json.IsNull(p) || bytes.Equal(p, jsonEmptyBytes) || bytes.Equal(p, jsonEmptyArrayBytes) {
			return
		}
		if !json.Validate(p) {
			err = errors.Warning("sql: write value failed").
				WithCause(fmt.Errorf("rv is not valid json bytes"))
			return
		}
		if w.ValueType.ConvertibleTo(bytesType) {
			rv.SetBytes(p)
			return
		}
		if w.ValueType.Kind() == reflect.Ptr {
			rv.Set(reflect.New(rv.Type().Elem()))
			decodeErr := json.Unmarshal(p, rv.Interface())
			if decodeErr != nil {
				err = errors.Warning("sql: write value failed").
					WithCause(fmt.Errorf("rv is not valid json bytes")).WithCause(decodeErr)
				return
			}
		} else {
			element := reflect.New(rv.Type()).Interface()
			decodeErr := json.Unmarshal(p, element)
			if decodeErr != nil {
				err = errors.Warning("sql: write value failed").
					WithCause(fmt.Errorf("rv is not valid json bytes")).WithCause(decodeErr)
				return
			}
			rv.Set(reflect.ValueOf(element).Elem())
		}
		return
	}
	err = errors.Warning("sql: write value failed").
		WithCause(fmt.Errorf("rv is not bytes"))
	return
}

type MappingValue struct {
	ValueType reflect.Type
}

func (w *MappingValue) Write(value any, rv reflect.Value) (err error) {
	p, ok := value.([]byte)
	if ok {
		if !json.Validate(p) {
			err = errors.Warning("sql: write value failed").
				WithCause(fmt.Errorf("rv is not valid json bytes"))
			return
		}
		if w.ValueType.Kind() == reflect.Ptr {
			rv.Set(reflect.New(rv.Type().Elem()))
			decodeErr := json.Unmarshal(p, rv.Interface())
			if decodeErr != nil {
				err = errors.Warning("sql: write value failed").
					WithCause(fmt.Errorf("rv is not valid json bytes")).WithCause(decodeErr)
				return
			}
		} else {
			element := reflect.New(rv.Type()).Interface()
			decodeErr := json.Unmarshal(p, element)
			if decodeErr != nil {
				err = errors.Warning("sql: write value failed").
					WithCause(fmt.Errorf("rv is not valid json bytes")).WithCause(decodeErr)
				return
			}
			rv.Set(reflect.ValueOf(element).Elem())
		}
		return
	}
	err = errors.Warning("sql: write value failed").
		WithCause(fmt.Errorf("rv is not bytes"))
	return
}

type ScanValue struct {
}

func (w *ScanValue) Write(value any, rv reflect.Value) (err error) {
	nv := reflect.New(reflect.Indirect(rv).Type())
	scanner, ok := nv.Interface().(stdsql.Scanner)
	if !ok {
		err = errors.Warning("sql: write value failed").
			WithCause(fmt.Errorf("rv is not sql.Scanner"))
		return
	}
	scanFieldValueErr := scanner.Scan(value)
	if scanFieldValueErr != nil {
		err = errors.Warning("sql: write value failed").
			WithCause(scanFieldValueErr)
		return
	}
	rv.Set(nv.Elem())
	return
}
