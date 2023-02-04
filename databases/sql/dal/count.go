package dal

import (
	"context"
	"fmt"
	"github.com/aacfactory/errors"
	"github.com/aacfactory/fns-contrib/databases/sql"
)

func Count[T Model](ctx context.Context, conditions *Conditions) (num int64, err errors.CodeError) {
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
		err = errors.ServiceError("dal: count failed").WithCause(queryErr)
		return
	}
	result, has := rows.Next()
	if !has {
		return
	}
	hasColumn, decodeErr := result.Column("_COUNT_", &num)
	if !hasColumn {
		err = errors.ServiceError("dal: count failed").WithCause(fmt.Errorf("no named '_COUNT_' column in results"))
		return
	}
	if decodeErr != nil {
		err = errors.ServiceError("dal: count failed").WithCause(decodeErr)
		return
	}
	return
}
