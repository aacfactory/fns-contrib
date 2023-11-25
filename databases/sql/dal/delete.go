package dal

import (
	"fmt"
	"github.com/aacfactory/errors"
	"github.com/aacfactory/fns-contrib/databases/sql"
	"github.com/aacfactory/fns/context"
	"reflect"
)

func Delete(ctx context.Context, model Model) (err error) {
	if model == nil {
		return
	}
	rv := reflect.ValueOf(model)
	if rv.Type().Kind() != reflect.Ptr {
		err = errors.Warning("dal: delete failed").WithCause(fmt.Errorf(" for type of model is not ptr"))
		return
	}
	structure, generator, getGeneratorErr := getModelQueryGenerator(ctx, model)
	if getGeneratorErr != nil {
		err = errors.Warning("dal: delete failed").WithCause(getGeneratorErr)
		return
	}
	// audit
	tryFillDeleteErr := tryFillAuditDelete(ctx, rv, structure)
	if tryFillDeleteErr != nil {
		err = errors.Warning("dal: delete failed").WithCause(tryFillDeleteErr)
		return
	}
	// generator
	_, query, arguments, generateErr := generator.Update(ctx, model)
	if generateErr != nil {
		err = errors.Warning("dal: delete failed").WithCause(generateErr)
		return
	}
	// handle
	result, executeErr := sql.Execute(ctx, query, arguments...)
	if executeErr != nil {
		err = errors.Warning("dal: delete failed").WithCause(executeErr)
		return
	}
	if result.RowsAffected == 0 {
		return
	}
	// version
	tryFillAOLErr := tryFillAOLField(rv, structure)
	if tryFillAOLErr != nil {
		err = errors.Warning("dal: delete failed").WithCause(tryFillAOLErr)
		return
	}
	return
}

func DeleteWithConditions[T Model](ctx context.Context, cond *Conditions) (affected int64, err error) {
	model := newModel[T]()
	_, generator, getGeneratorErr := getModelQueryGenerator(ctx, model)
	if getGeneratorErr != nil {
		err = errors.Warning("dal: delete failed").WithCause(getGeneratorErr)
		return
	}
	_, query, args, genErr := generator.DeleteWithConditions(ctx, cond)
	if genErr != nil {
		err = errors.Warning("dal: delete failed").WithCause(genErr)
		return
	}
	result, execErr := sql.Execute(ctx, query, args)
	if execErr != nil {
		err = errors.Warning("dal: delete failed").WithCause(execErr)
		return
	}
	affected = result.RowsAffected
	return
}
