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

func NewValues() Values {
	return make([]Value, 0, 1)
}

type Values []Value

func (values Values) Append(field string, value interface{}) Values {
	return append(values, Value{
		Field: field,
		Value: value,
	})
}

func NewUnpreparedValue(fragment string) *UnpreparedValue {
	return &UnpreparedValue{
		Fragment: fragment,
	}
}

type UnpreparedValue struct {
	Fragment string
}

type Value struct {
	Field string
	Value interface{}
}

func UpdateWithValues[T Model](ctx context.Context, values Values, cond *Conditions) (affected int64, err errors.CodeError) {
	if values == nil || len(values) == 0 {
		err = errors.ServiceError("dal: update failed").WithCause(errors.Warning("values is required"))
		return
	}
	model := newModel[T]()
	_, generator, getGeneratorErr := getModelQueryGenerator(ctx, model)
	if getGeneratorErr != nil {
		err = errors.ServiceError("dal: update failed").WithCause(getGeneratorErr)
		return
	}
	_, query, args, genErr := generator.UpdateWithValues(ctx, values, cond)
	if genErr != nil {
		err = errors.ServiceError("dal: update failed").WithCause(genErr)
		return
	}
	affected, _, err = sql.Execute(ctx, query, args)
	if err != nil {
		err = errors.ServiceError("dal: update failed").WithCause(err)
		return
	}
	return
}
