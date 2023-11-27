package dac

import (
	"github.com/aacfactory/fns-contrib/databases/sql/dac/conditions"
	"github.com/aacfactory/fns-contrib/databases/sql/dac/specifications"
	"github.com/aacfactory/fns/context"
)

func Update[T Table](ctx context.Context, entry T) (v T, err error) {

	return
}

func Field(name string, value any) FieldValues {
	return FieldValues{{name, value}}
}

type FieldValues []specifications.FieldValue

func (fields FieldValues) Field(name string, value any) FieldValues {
	return append(fields, specifications.FieldValue{
		Name: name, Value: value,
	})
}

func UpdateField[T Table](ctx context.Context, fields FieldValues, cond conditions.Condition) (v T, err error) {
	// todo 在 append arguments 时，注意对应field的类型，如果是json，直接encode
	return
}
