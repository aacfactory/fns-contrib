package dal

import (
	"context"
	"github.com/aacfactory/errors"
	"github.com/aacfactory/fns-contrib/databases/sql"
)

func Exist[T Model](ctx context.Context, conditions *Conditions) (has bool, err errors.CodeError) {
	model := newModel[T]()
	_, generator, getGeneratorErr := getModelQueryGenerator(ctx, model)
	if getGeneratorErr != nil {
		err = errors.Warning("dal: exist failed").WithCause(getGeneratorErr)
		return
	}
	// generator
	_, query, arguments, generateErr := generator.Exist(ctx, conditions)
	if generateErr != nil {
		err = errors.Warning("dal: exist failed").WithCause(generateErr)
		return
	}
	// handle
	rows, queryErr := sql.Query(ctx, query, arguments...)
	if queryErr != nil {
		err = errors.ServiceError("dal: exist failed").WithCause(queryErr)
		return
	}
	has = !rows.Empty()
	return
}
