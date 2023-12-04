package specifications

import (
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

func ScanRows[T any](ctx context.Context, rows sql.Rows, columns []string) (entries []T, err error) {
	spec, specErr := GetSpecification(ctx, Instance[T]())
	if specErr != nil {
		err = specErr
		return
	}
	for rows.Next() {
		fields := make([]any, 0, len(columns))
		for _, fieldName := range columns {
			column, hasColumn := spec.ColumnByField(fieldName)
			if !hasColumn {
				err = errors.Warning("sql: field was not found").WithMeta("field", fieldName).WithMeta("table", spec.Key)
				return
			}
			columnValue, columnValueErr := column.ScanValue()
			if columnValueErr != nil {
				err = errors.Warning("sql: scan rows failed").WithCause(columnValueErr).WithMeta("table", spec.Key).WithMeta("field", column.Field)
				return
			}
			fields = append(fields, &columnValue)
		}
		scanErr := rows.Scan(fields...)
		if scanErr != nil {
			err = scanErr
			return
		}
		entry := Instance[T]()
		rv := reflect.Indirect(reflect.ValueOf(&entry))
		for i, field := range fields {
			if field == nil {
				continue
			}
			fieldValue := reflect.Indirect(reflect.ValueOf(field))
			if fieldValue.IsNil() {
				continue
			}
			fieldName := columns[i]
			fv := rv.FieldByName(fieldName)
			column, _ := spec.ColumnByField(fieldName)
			fieldErr := ScanColumn(column, field, fv)
			if fieldErr != nil {
				err = fieldErr
				return
			}
		}
		entries = append(entries, entry)
	}
	return
}

func ScanColumn(column *Column, columnPtrValue any, field reflect.Value) (err error) {
	value := reflect.Indirect(reflect.ValueOf(columnPtrValue))
	if value.IsNil() {
		return
	}
	columnValue := value.Interface()
	switch column.Type.Name {
	case StringType:
		vv, ok := columnValue.(string)
		if ok {
			if field.Type().Kind() == reflect.String {
				field.SetString(vv)
			} else if field.Type().ConvertibleTo(nullStringType) {
				field.Set(reflect.ValueOf(stdsql.NullString{
					String: vv,
					Valid:  vv != "",
				}).Convert(field.Type()))
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
		vv, ok := columnValue.(bool)
		if ok {
			if field.Type().Kind() == reflect.Bool {
				field.SetBool(vv)
			} else if field.Type().ConvertibleTo(nullStringType) {
				field.Set(reflect.ValueOf(stdsql.NullBool{
					Bool:  vv,
					Valid: true,
				}).Convert(field.Type()))
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
		n, ok := AsInt(columnValue)
		if ok {
			switch field.Type().Kind() {
			case reflect.Int64, reflect.Int, reflect.Int32, reflect.Int16, reflect.Int8:
				field.SetInt(n)
				break
			default:
				if field.Type().ConvertibleTo(nullInt64Type) {
					field.Set(reflect.ValueOf(stdsql.NullInt64{
						Int64: n,
						Valid: true,
					}).Convert(field.Type()))
				} else if field.Type().ConvertibleTo(nullInt32Type) {
					field.Set(reflect.ValueOf(stdsql.NullInt32{
						Int32: int32(n),
						Valid: true,
					}).Convert(field.Type()))
				} else if field.Type().ConvertibleTo(nullInt16Type) {
					field.Set(reflect.ValueOf(stdsql.NullInt16{
						Int16: int16(n),
						Valid: true,
					}).Convert(field.Type()))
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
		f, ok := AsFloat(columnValue)
		if ok {
			switch field.Type().Kind() {
			case reflect.Float64, reflect.Float32:
				field.SetFloat(f)
				break
			default:
				if field.Type().ConvertibleTo(nullFloatType) {
					field.Set(reflect.ValueOf(stdsql.NullFloat64{
						Float64: f,
						Valid:   true,
					}).Convert(field.Type()))
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
		b, ok := columnValue.(byte)
		if ok {
			switch field.Type().Kind() {
			case reflect.Uint8:
				field.Set(reflect.ValueOf(b))
				break
			default:
				if field.Type().ConvertibleTo(nullByteType) {
					field.Set(reflect.ValueOf(stdsql.NullByte{
						Byte:  b,
						Valid: true,
					}).Convert(field.Type()))
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
		p, ok := columnValue.([]byte)
		if ok {
			if field.Type().ConvertibleTo(bytesType) {
				field.Set(reflect.ValueOf(p).Convert(field.Type()))
			} else if field.Type().ConvertibleTo(nullBytesType) {
				field.Set(reflect.ValueOf(sql.NullBytes{
					Bytes: p,
					Valid: true,
				}).Convert(field.Type()))
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
		t, ok := columnValue.(time.Time)
		if ok {
			if field.Type().ConvertibleTo(timeType) {
				field.Set(reflect.ValueOf(t).Convert(field.Type()))
			} else if field.Type().ConvertibleTo(nullTimeType) {
				field.Set(reflect.ValueOf(stdsql.NullTime{
					Time:  t,
					Valid: !t.IsZero(),
				}).Convert(field.Type()))
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
		t, ok := columnValue.(time.Time)
		if ok {
			field.Set(reflect.ValueOf(times.DataOf(t)))
			break
		}
		err = errors.Warning("sql: scan rows failed").
			WithCause(fmt.Errorf("value is not time.Time")).
			WithMeta("name", column.Name)
		return
	case TimeType:
		t, ok := columnValue.(time.Time)
		if ok {
			field.Set(reflect.ValueOf(times.TimeOf(t)))
			break
		}
		err = errors.Warning("sql: scan rows failed").
			WithCause(fmt.Errorf("value is not time.Time")).
			WithMeta("name", column.Name)
		return
	case JsonType, MappingType:
		p, ok := columnValue.([]byte)
		if ok {
			if json.IsNull(p) {
				break
			}
			if !json.Validate(p) {
				err = errors.Warning("sql: scan rows failed").
					WithCause(fmt.Errorf("value is not valid json bytes")).
					WithMeta("name", column.Name)
				return
			}
			if column.Type.Value.ConvertibleTo(bytesType) {
				field.SetBytes(p)
				break
			}
			if column.Type.Value.Kind() == reflect.Ptr {
				field.Set(reflect.New(field.Type().Elem()))
				decodeErr := json.Unmarshal(p, field.Interface())
				if decodeErr != nil {
					err = errors.Warning("sql: scan rows failed").
						WithCause(fmt.Errorf("value is not valid json bytes")).WithCause(decodeErr).
						WithMeta("name", column.Name)
					return
				}
			} else {
				element := reflect.New(field.Type()).Interface()
				decodeErr := json.Unmarshal(p, element)
				if decodeErr != nil {
					err = errors.Warning("sql: scan rows failed").
						WithCause(fmt.Errorf("value is not valid json bytes")).WithCause(decodeErr).
						WithMeta("name", column.Name)
					return
				}
				field.Set(reflect.ValueOf(element).Elem())
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
		scanFieldValueErr := scanner.Scan(reflect.ValueOf(columnValue).Elem().Interface())
		if scanFieldValueErr != nil {
			err = errors.Warning("sql: scan rows failed").
				WithCause(fmt.Errorf("scan field value failed")).WithCause(scanFieldValueErr).
				WithMeta("name", column.Name)
			return
		}
		field.Set(reflect.ValueOf(scanner))
		break
	default:
		err = errors.Warning("sql: scan rows failed").
			WithCause(fmt.Errorf("type of field is invalid")).
			WithMeta("name", column.Name)
		return
	}
	return
}
