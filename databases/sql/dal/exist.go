package dal

import (
	"github.com/aacfactory/errors"
	"github.com/aacfactory/fns-contrib/databases/sql"
	"github.com/aacfactory/fns/context"
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
		err = errors.Warning("dal: exist failed").WithCause(queryErr)
		return
	}
	if rows.Next() {
		has = true
	}
	_ = rows.Close()
	return
}
