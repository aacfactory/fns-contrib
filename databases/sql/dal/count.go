package dal

import (
	"github.com/aacfactory/errors"
	"github.com/aacfactory/fns-contrib/databases/sql"
	"github.com/aacfactory/fns/context"
)

func Count[T Model](ctx context.Context, conditions *Conditions) (num int64, err error) {
	model := newModel[T]()
	_, generator, getGeneratorErr := getModelQueryGenerator(ctx, model)
	if getGeneratorErr != nil {
		err = errors.Warning("dal: count failed").WithCause(getGeneratorErr)
		return
	}
	// generator
	_, query, arguments, generateErr := generator.Count(ctx, conditions)
	if generateErr != nil {
		err = errors.Warning("dal: count failed").WithCause(generateErr)
		return
	}
	// handle
	rows, queryErr := sql.Query(ctx, query, arguments...)
	if queryErr != nil {
		err = errors.Warning("dal: count failed").WithCause(queryErr)
		return
	}
	if rows.Next() {
		scanErr := rows.Scan(&num)
		if scanErr != nil {
			_ = rows.Close()
			err = errors.Warning("dal: count failed").WithCause(scanErr)
			return
		}
	}
	_ = rows.Close()
	return
}
