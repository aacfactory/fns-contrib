package postgres

import (
	"github.com/aacfactory/fns-contrib/databases/postgres/dialect"
	"github.com/aacfactory/fns-contrib/databases/sql"
	"github.com/aacfactory/fns-contrib/databases/sql/dac"
	"github.com/aacfactory/fns-contrib/databases/sql/dac/conditions"
	"github.com/aacfactory/fns-contrib/databases/sql/dac/specifications"
	"github.com/aacfactory/fns/context"
)

func Update[T Table](ctx context.Context, entry T) (v T, affected int64, err error) {
	sql.ForceDialect(ctx, dialect.Name)
	v, affected, err = dac.Update[T](ctx, entry)
	return
}

func Field(name string, value any) FieldValues {
	return FieldValues{{name, value}}
}

type FieldValues dac.FieldValues

func (fields FieldValues) Field(name string, value any) FieldValues {
	return append(fields, specifications.FieldValue{
		Name: name, Value: value,
	})
}

func UpdateFields[T Table](ctx context.Context, fields FieldValues, cond conditions.Condition) (affected int64, err error) {
	sql.ForceDialect(ctx, dialect.Name)
	affected, err = dac.UpdateFields[T](ctx, dac.FieldValues(fields), cond)
	return
}
