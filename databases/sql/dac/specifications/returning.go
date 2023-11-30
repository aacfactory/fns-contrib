package specifications

import (
	"context"
	"fmt"
	"github.com/aacfactory/errors"
	"github.com/aacfactory/fns-contrib/databases/sql"
	"github.com/aacfactory/json"
	"reflect"
)

func WriteInsertReturning[T Table](ctx context.Context, rows sql.Rows, returning []int, entries []T) (affected int64, err error) {
	spec, specErr := GetSpecification(ctx, TableInstance[T]())
	if specErr != nil {
		err = errors.Warning("sql: write returning value into entries failed").WithCause(specErr)
		return
	}
	rowValues := make([][]any, 0, len(entries))
	for rows.Next() {
		items := make([]any, 0, 1)
		for _, rfi := range returning {
			column, hasColumn := spec.ColumnByFieldIdx(rfi)
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

	if len(spec.Conflicts) == 0 {
		if affected == int64(len(entries)) {
			for i, entry := range entries {
				row := rowValues[i]
				rv := reflect.Indirect(reflect.ValueOf(&entry))
				for j, item := range row {
					fieldIdx := returning[j]
					fv := rv.Field(fieldIdx)
					switch f := item.(type) {
					case ScanValue:
						fsv, valid := f.Value()
						if valid {
							column, _ := spec.ColumnByFieldIdx(fieldIdx)
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
	} else {
		conflicts, conflictsErr := spec.ConflictColumns()
		if conflictsErr != nil {
			err = errors.Warning("sql: write returning value into entries failed").WithCause(conflictsErr)
			return
		}
		pos := len(returning)
		tmpConflicts := make([]*Column, 0, len(conflicts))
		for i, idx := range returning {
			matched := -1
			for j, conflict := range conflicts {
				if conflict.FieldIdx == idx {
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
					if reflect.Indirect(rv.Field(conflicts[j].FieldIdx)).Equal(reflect.Indirect(reflect.ValueOf(value))) {
						matched++
					}
				}
				if matched == len(conflictValues) {
					for j, item := range items {
						fieldIdx := returning[j]
						fv := rv.Field(fieldIdx)
						switch f := item.(type) {
						case ScanValue:
							fsv, valid := f.Value()
							if valid {
								column, _ := spec.ColumnByFieldIdx(fieldIdx)
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