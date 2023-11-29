package specifications

import (
	"context"
	"github.com/aacfactory/errors"
	"github.com/aacfactory/fns-contrib/databases/sql"
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
			// todo conflict(last of returning)
			ct := column.Type.Value
			switch ct.Kind() {
			case reflect.Ptr:
			case reflect.Struct:

			}
		}

		rowValues = append(rowValues, items)
		affected++
	}
	return
}
