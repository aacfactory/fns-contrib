package specifications

import (
	"context"
	stdsql "database/sql"
	"github.com/aacfactory/errors"
	"github.com/aacfactory/fns-contrib/databases/sql"
	"github.com/aacfactory/json"
	"reflect"
	"strconv"
)

func ScanRows[T Table](ctx context.Context, rows sql.Rows, columns []int) (entries []T, err error) {
	spec, specErr := GetSpecification(ctx, TableInstance[T]())
	if specErr != nil {
		err = specErr
		return
	}
	for rows.Next() {
		entry := TableInstance[T]()
		fields := make([]any, 0, len(columns))
		jsonFields := make([]int, 0, 1)
		rv := reflect.Indirect(reflect.ValueOf(entry))
		for _, fieldIdx := range columns {
			column, hasColumn := spec.ColumnByFieldIdx(fieldIdx)
			if !hasColumn {
				err = errors.Warning("sql: field was not found").WithMeta("fieldIdx", strconv.Itoa(fieldIdx)).WithMeta("table", spec.Key)
				return
			}
			if column.Type.Value == jsonRawMessageType {
				fields = append(fields, &stdsql.RawBytes{})
				jsonFields = append(jsonFields, len(fields)-1)
				continue
			}
			// scanner or Unmarshaler
			if column.Type.Value.Implements(scannerType) || column.Type.Value.Implements(jsonUnmarshalerType) {
				switch column.Type.Value.Kind() {
				case reflect.Ptr:
					fields = append(fields, reflect.New(column.Type.Value.Elem()).Interface())
					break
				case reflect.Struct:
					fields = append(fields, reflect.New(column.Type.Value).Interface())
					break
				case reflect.Slice:
					v := reflect.MakeSlice(column.Type.Value.Elem(), 0, 1).Interface()
					fields = append(fields, &v)
					break
				case reflect.Map:
					v := reflect.MakeMap(column.Type.Value).Interface()
					fields = append(fields, &v)
					break
				default:
					err = errors.Warning("sql: field is not be scanned").WithMeta("field", column.Field).WithMeta("table", spec.Key)
					return
				}
				continue
			}
			// json or normal
			switch column.Type.Name {
			case JsonType, MappingType:
				fields = append(fields, &stdsql.RawBytes{})
				jsonFields = append(jsonFields, len(fields)-1)
				break
			default:
				v := rv.Field(fieldIdx).Interface()
				fields = append(fields, &v)
				break
			}
		}
		scanErr := rows.Scan(fields...)
		if scanErr != nil {
			err = scanErr
			return
		}
		for i, field := range fields {
			fieldIdx := columns[i]

			isJson := false
			for _, jsonField := range jsonFields {
				if jsonField == i {
					isJson = true
					break
				}
			}

			fv := rv.Field(fieldIdx)
			if isJson {
				raw := field.(*stdsql.RawBytes)
				fi := reflect.Indirect(fv).Interface()
				decodeErr := json.Unmarshal(*raw, &fi)
				if decodeErr != nil {
					err = errors.Warning("sql: Unmarshal failed").WithCause(decodeErr).WithMeta("field", rv.Type().Field(fieldIdx).Name).WithMeta("table", spec.Key)
					return
				}
				continue
			}

			fv.Set(reflect.Indirect(reflect.ValueOf(field)))
		}
		entries = append(entries, entry)
	}
	return
}
