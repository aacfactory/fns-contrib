package specifications

import (
	"context"
	"github.com/aacfactory/fns-contrib/databases/sql"
	"reflect"
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
		rv := reflect.Indirect(reflect.ValueOf(entry))
		for _, column := range columns {
			fieldIdx := spec.Columns[column].FieldIdx
			fv := rv.Field(fieldIdx)
			switch fv.Type().Kind() {
			case reflect.Ptr:
				nfv := reflect.New(fv.Type().Elem())
				fv.Set(nfv)
				fields = append(fields, fv.Interface())
				break
			case reflect.Slice:
				nfv := reflect.MakeSlice(fv.Type().Elem(), 0, 1)
				fv.Set(nfv)
				fvi := fv.Interface()
				fields = append(fields, &fvi)
				break
			case reflect.Map:
				nfv := reflect.MakeMap(fv.Type())
				fv.Set(nfv)
				fvi := fv.Interface()
				fields = append(fields, &fvi)
				break
			default:
				fvi := fv.Interface()
				fields = append(fields, &fvi)
				break
			}
		}
		scanErr := rows.Scan(fields...)
		if scanErr != nil {
			err = scanErr
			return
		}
		entries = append(entries, entry)
	}
	return
}
