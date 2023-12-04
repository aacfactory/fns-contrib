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

func WriteInsertReturning[T any](ctx context.Context, rows sql.Rows, returning []string, entries []T) (affected int64, err error) {
	spec, specErr := GetSpecification(ctx, Instance[T]())
	if specErr != nil {
		err = errors.Warning("sql: write returning value into entries failed").WithCause(specErr)
		return
	}
	rowValues := make([][]any, 0, len(entries))
	for rows.Next() {
		items := make([]any, 0, 1)
		for _, rfn := range returning {
			column, hasColumn := spec.ColumnByField(rfn)
			if !hasColumn {
				err = errors.Warning("sql: write returning value into entries failed").WithCause(specErr)
				return
			}
			columnValue, columnValueErr := column.ScanValue()
			if columnValueErr != nil {
				err = errors.Warning("sql: write returning value into entries failed").WithCause(columnValueErr)
				return
			}
			items = append(items, &columnValue)
		}
		scanErr := rows.Scan(items...)
		if scanErr != nil {
			err = errors.Warning("sql: write returning value into entries failed").WithCause(scanErr)
			return
		}

		rowValues = append(rowValues, items)
		affected++
	}

	if affected == int64(len(entries)) {
		for i, entry := range entries {
			row := rowValues[i]
			rv := reflect.Indirect(reflect.ValueOf(&entry))
			for j, cell := range row {
				item := reflect.Indirect(reflect.ValueOf(cell)).Interface()
				fieldName := returning[j]
				fv := rv.FieldByName(fieldName)
				ft, _ := rv.Type().FieldByName(fieldName)
				column, _ := spec.ColumnByField(fieldName)
				switch column.Type.Name {
				case StringType:
					vv, ok := item.(string)
					if ok {
						fv.SetString(vv)
						break
					}
					err = errors.Warning("sql: write returning value into entries failed").
						WithCause(fmt.Errorf("value is not string")).
						WithMeta("name", column.Name)
					return
				case BoolType:
					vv, ok := item.(bool)
					if ok {
						fv.SetBool(vv)
						break
					}
					err = errors.Warning("sql: write returning value into entries failed").
						WithCause(fmt.Errorf("value is not string")).
						WithMeta("name", column.Name)
					return
				case IntType:
					ii, ok := item.(int)
					if ok {
						fv.SetInt(int64(ii))
						break
					}
					i8, i8ok := item.(int8)
					if i8ok {
						fv.SetInt(int64(i8))
						break
					}
					i16, i16ok := item.(int16)
					if i16ok {
						fv.SetInt(int64(i16))
						break
					}
					i32, i32ok := item.(int32)
					if i32ok {
						fv.SetInt(int64(i32))
						break
					}
					i64, i64ok := item.(int64)
					if i64ok {
						fv.SetInt(i64)
						break
					}
					err = errors.Warning("sql: write returning value into entries failed").
						WithCause(fmt.Errorf("value is not int")).
						WithMeta("name", column.Name)
					return
				case FloatType:
					f32, f32ok := item.(float32)
					if f32ok {
						fv.SetFloat(float64(f32))
						break
					}
					f64, f64ok := item.(float64)
					if f64ok {
						fv.SetFloat(f64)
						break
					}
					err = errors.Warning("sql: write returning value into entries failed").
						WithCause(fmt.Errorf("value is not float")).
						WithMeta("name", column.Name)
					return
				case ByteType:
					b, ok := item.(byte)
					if ok {
						fv.Set(reflect.ValueOf(b))
						break
					}
					err = errors.Warning("sql: write returning value into entries failed").
						WithCause(fmt.Errorf("value is not byte")).
						WithMeta("name", column.Name)
					return
				case BytesType:
					p, ok := item.([]byte)
					if ok {
						fv.SetBytes(p)
						break
					}
					err = errors.Warning("sql: write returning value into entries failed").
						WithCause(fmt.Errorf("value is not bytes")).
						WithMeta("name", column.Name)
					return
				case DatetimeType:
					t, ok := item.(time.Time)
					if ok {
						fv.Set(reflect.ValueOf(t))
						break
					}
					err = errors.Warning("sql: write returning value into entries failed").
						WithCause(fmt.Errorf("value is not time.Time")).
						WithMeta("name", column.Name)
					return
				case DateType:
					t, ok := item.(time.Time)
					if ok {
						fv.Set(reflect.ValueOf(times.DataOf(t)))
						break
					}
					err = errors.Warning("sql: write returning value into entries failed").
						WithCause(fmt.Errorf("value is not time.Time")).
						WithMeta("name", column.Name)
					return
				case TimeType:
					t, ok := item.(time.Time)
					if ok {
						fv.Set(reflect.ValueOf(times.TimeOf(t)))
						break
					}
					err = errors.Warning("sql: write returning value into entries failed").
						WithCause(fmt.Errorf("value is not time.Time")).
						WithMeta("name", column.Name)
					return
				case JsonType, MappingType:
					p, ok := item.([]byte)
					if ok {
						if json.IsNull(p) {
							break
						}
						if !json.Validate(p) {
							err = errors.Warning("sql: write returning value into entries failed").
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
								err = errors.Warning("sql: write returning value into entries failed").
									WithCause(fmt.Errorf("value is not valid json bytes")).WithCause(decodeErr).
									WithMeta("name", column.Name)
								return
							}
						} else {
							element := reflect.New(ft.Type).Interface()
							decodeErr := json.Unmarshal(p, element)
							if decodeErr != nil {
								err = errors.Warning("sql: write returning value into entries failed").
									WithCause(fmt.Errorf("value is not valid json bytes")).WithCause(decodeErr).
									WithMeta("name", column.Name)
								return
							}
							fv.Set(reflect.ValueOf(element).Elem())
						}
						break
					}
					err = errors.Warning("sql: write returning value into entries failed").
						WithCause(fmt.Errorf("value is not bytes")).
						WithMeta("name", column.Name)
					return
				case ScanType:
					scanner, ok := column.PtrValue().(stdsql.Scanner)
					if !ok {
						err = errors.Warning("sql: write returning value into entries failed").
							WithCause(fmt.Errorf("field is not sql.Scanner")).
							WithMeta("name", column.Name)
						return
					}
					scanFieldValueErr := scanner.Scan(reflect.ValueOf(item).Elem().Interface())
					if scanFieldValueErr != nil {
						err = errors.Warning("sql: write returning value into entries failed").
							WithCause(fmt.Errorf("scan field value failed")).WithCause(scanFieldValueErr).
							WithMeta("name", column.Name)
						return
					}
					break
				default:
					err = errors.Warning("sql: write returning value into entries failed").
						WithCause(fmt.Errorf("type of field is invalid")).
						WithMeta("name", column.Name)
					return
				}
			}
			entries[i] = entry
		}
		return
	}

	if len(spec.Conflicts) > 0 {
		conflicts, conflictsErr := spec.ConflictColumns()
		if conflictsErr != nil {
			err = errors.Warning("sql: write returning value into entries failed").WithCause(conflictsErr)
			return
		}
		pos := len(returning)
		tmpConflicts := make([]*Column, 0, len(conflicts))
		for i, fieldName := range returning {
			matched := -1
			for j, conflict := range conflicts {
				if conflict.Field == fieldName {
					matched = j
					if pos > i {
						pos = i
					}
					break
				}
			}
			if matched > 0 {
				tmpConflicts = append(tmpConflicts, conflicts[matched])
			}
		}
		conflicts = tmpConflicts
		if pos == len(returning) {
			err = errors.Warning("sql: write returning value into entries failed").WithCause(fmt.Errorf("there is no conflict column in returning"))
			return
		}
		if pos == 0 {
			err = errors.Warning("sql: write returning value into entries failed").WithCause(fmt.Errorf("there is no valid column in returning"))
			return
		}

		for _, row := range rowValues {
			items := row[0:pos]
			conflictValues := row[pos:]
			for i, entry := range entries {
				rv := reflect.Indirect(reflect.ValueOf(&entry))
				matched := 0
				for j, value := range conflictValues {
					if reflect.Indirect(rv.FieldByName(conflicts[j].Field)).Equal(reflect.Indirect(reflect.ValueOf(value))) {
						matched++
					}
				}
				if matched == len(conflictValues) {
					for j, item := range items {
						fieldName := returning[j]
						fv := rv.FieldByName(fieldName)
						switch f := item.(type) {
						case ScanValue:
							fsv, valid := f.Value()
							if valid {
								column, _ := spec.ColumnByField(fieldName)
								switch column.Type.Name {
								case DateType:
									fv.Set(reflect.ValueOf(fsv))
									break
								case TimeType:
									fv.Set(reflect.ValueOf(fsv))
									break
								case JsonType:
									cv := column.ZeroValue()
									decodeErr := json.Unmarshal(fsv.(json.RawMessage), &cv)
									if decodeErr != nil {
										err = errors.Warning("sql: scan rows failed").WithCause(decodeErr).WithMeta("table", spec.Key).WithMeta("field", column.Field)
										return
									}
									break
								case MappingType:
									cv := column.ZeroValue()
									decodeErr := json.Unmarshal(fsv.(json.RawMessage), &cv)
									if decodeErr != nil {
										err = errors.Warning("sql: scan rows failed").WithCause(decodeErr).WithMeta("table", spec.Key).WithMeta("field", column.Field)
										return
									}
									break
								}
							}
							break
						default:
							fv.Set(reflect.ValueOf(f))
						}
					}
					entries[i] = entry
				}
			}
		}
	}
	return
}
