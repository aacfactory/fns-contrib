package specifications

import (
	"context"
	"github.com/aacfactory/errors"
	"github.com/aacfactory/fns-contrib/databases/sql"
	"github.com/aacfactory/json"
	"reflect"
	"strconv"
)

func ScanRows[T any](ctx context.Context, rows sql.Rows, columns []int) (entries []T, err error) {
	spec, specErr := GetSpecification(ctx, Instance[T]())
	if specErr != nil {
		err = specErr
		return
	}
	for rows.Next() {
		fields := make([]any, 0, len(columns))
		for _, fieldIdx := range columns {
			column, hasColumn := spec.ColumnByFieldIdx(fieldIdx)
			if !hasColumn {
				err = errors.Warning("sql: field was not found").WithMeta("fieldIdx", strconv.Itoa(fieldIdx)).WithMeta("table", spec.Key)
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
			fieldIdx := columns[i]
			fv := rv.Field(fieldIdx)
			switch f := field.(type) {
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
		entries = append(entries, entry)
	}
	return
}
