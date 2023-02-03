package dal

import (
	"context"
	"fmt"
	"github.com/aacfactory/errors"
	"github.com/aacfactory/fns-contrib/databases/sql"
	"reflect"
)

func Update(ctx context.Context, model Model) (err errors.CodeError) {
	if model == nil {
		return
	}
	rv := reflect.ValueOf(model)
	if rv.Type().Kind() != reflect.Ptr {
		err = errors.ServiceError("dal: update failed").WithCause(fmt.Errorf(" for type of model is not ptr"))
		return
	}
	structure, generator, getGeneratorErr := getModelQueryGenerator(ctx, model)
	if getGeneratorErr != nil {
		err = errors.Warning("dal: update failed").WithCause(getGeneratorErr)
		return
	}
	// audit
	tryFillModifyErr := tryFillAuditModify(ctx, rv, structure)
	if tryFillModifyErr != nil {
		err = errors.ServiceError("dal: update failed").WithCause(tryFillModifyErr)
		return
	}
	// generator
	_, query, arguments, generateErr := generator.Update(ctx, model)
	if generateErr != nil {
		err = errors.Warning("dal: update failed").WithCause(generateErr)
		return
	}
	// handle
	affected, _, executeErr := sql.Execute(ctx, query, arguments...)
	if executeErr != nil {
		err = errors.ServiceError("dal: update failed").WithCause(executeErr)
		return
	}
	if affected == 0 {
		return
	}
	// version
	tryFillAOLErr := tryFillAOLField(rv, structure)
	if tryFillAOLErr != nil {
		err = errors.ServiceError("dal: update failed").WithCause(tryFillAOLErr)
		return
	}
	return
}
