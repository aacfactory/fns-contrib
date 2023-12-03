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
			fi := fieldValue.Interface()
			fieldName := columns[i]
			ft, _ := rv.Type().FieldByName(fieldName)
			fv := rv.FieldByName(fieldName)
			column, _ := spec.ColumnByField(fieldName)
			switch column.Type.Name {
			case StringType:
				vv, ok := fi.(string)
				if ok {
					fv.SetString(vv)
					break
				}
				err = errors.Warning("sql: scan rows failed").
					WithCause(fmt.Errorf("value is not string")).
					WithMeta("name", column.Name)
				return
			case BoolType:
				vv, ok := fi.(bool)
				if ok {
					fv.SetBool(vv)
					break
				}
				err = errors.Warning("sql: scan rows failed").
					WithCause(fmt.Errorf("value is not string")).
					WithMeta("name", column.Name)
				return
			case IntType:
				ii, ok := fi.(int)
				if ok {
					fv.SetInt(int64(ii))
					break
				}
				i8, i8ok := fi.(int8)
				if i8ok {
					fv.SetInt(int64(i8))
					break
				}
				i16, i16ok := fi.(int16)
				if i16ok {
					fv.SetInt(int64(i16))
					break
				}
				i32, i32ok := fi.(int32)
				if i32ok {
					fv.SetInt(int64(i32))
					break
				}
				i64, i64ok := fi.(int64)
				if i64ok {
					fv.SetInt(i64)
					break
				}
				err = errors.Warning("sql: scan rows failed").
					WithCause(fmt.Errorf("value is not int")).
					WithMeta("name", column.Name)
				return
			case FloatType:
				f32, f32ok := fi.(float32)
				if f32ok {
					fv.SetFloat(float64(f32))
					break
				}
				f64, f64ok := fi.(float64)
				if f64ok {
					fv.SetFloat(f64)
					break
				}
				err = errors.Warning("sql: scan rows failed").
					WithCause(fmt.Errorf("value is not float")).
					WithMeta("name", column.Name)
				return
			case ByteType:
				b, ok := fi.(byte)
				if ok {
					fv.Set(reflect.ValueOf(b))
					break
				}
				err = errors.Warning("sql: scan rows failed").
					WithCause(fmt.Errorf("value is not byte")).
					WithMeta("name", column.Name)
				return
			case BytesType:
				p, ok := fi.([]byte)
				if ok {
					fv.SetBytes(p)
					break
				}
				err = errors.Warning("sql: scan rows failed").
					WithCause(fmt.Errorf("value is not bytes")).
					WithMeta("name", column.Name)
				return
			case DatetimeType:
				t, ok := fi.(time.Time)
				if ok {
					fv.Set(reflect.ValueOf(t))
					break
				}
				err = errors.Warning("sql: scan rows failed").
					WithCause(fmt.Errorf("value is not time.Time")).
					WithMeta("name", column.Name)
				return
			case DateType:
				t, ok := fi.(time.Time)
				if ok {
					fv.Set(reflect.ValueOf(times.DataOf(t)))
					break
				}
				err = errors.Warning("sql: scan rows failed").
					WithCause(fmt.Errorf("value is not time.Time")).
					WithMeta("name", column.Name)
				return
			case TimeType:
				t, ok := fi.(time.Time)
				if ok {
					fv.Set(reflect.ValueOf(times.TimeOf(t)))
					break
				}
				err = errors.Warning("sql: scan rows failed").
					WithCause(fmt.Errorf("value is not time.Time")).
					WithMeta("name", column.Name)
				return
			case JsonType, MappingType:
				p, ok := fi.([]byte)
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
						fv.SetBytes(p)
						break
					}
					if column.Type.Value.Kind() == reflect.Ptr {
						fv.Set(reflect.New(ft.Type.Elem()))
						decodeErr := json.Unmarshal(p, fv.Interface())
						if decodeErr != nil {
							err = errors.Warning("sql: scan rows failed").
								WithCause(fmt.Errorf("value is not valid json bytes")).WithCause(decodeErr).
								WithMeta("name", column.Name)
							return
						}
					} else {
						element := reflect.New(ft.Type).Interface()
						decodeErr := json.Unmarshal(p, element)
						if decodeErr != nil {
							err = errors.Warning("sql: scan rows failed").
								WithCause(fmt.Errorf("value is not valid json bytes")).WithCause(decodeErr).
								WithMeta("name", column.Name)
							return
						}
						fv.Set(reflect.ValueOf(element).Elem())
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
				scanFieldValueErr := scanner.Scan(reflect.ValueOf(fi).Elem().Interface())
				if scanFieldValueErr != nil {
					err = errors.Warning("sql: scan rows failed").
						WithCause(fmt.Errorf("scan field value failed")).WithCause(scanFieldValueErr).
						WithMeta("name", column.Name)
					return
				}
				break
			default:
				err = errors.Warning("sql: scan rows failed").
					WithCause(fmt.Errorf("type of field is invalid")).
					WithMeta("name", column.Name)
				return
			}
		}
		entries = append(entries, entry)
	}
	return
}
