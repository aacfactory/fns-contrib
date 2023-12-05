package dac

import (
	"github.com/aacfactory/errors"
	"github.com/aacfactory/fns-contrib/databases/sql"
	"github.com/aacfactory/fns-contrib/databases/sql/dac/conditions"
	"github.com/aacfactory/fns-contrib/databases/sql/dac/specifications"
	"github.com/aacfactory/fns/context"
)

func Update[T Table](ctx context.Context, entry T) (v T, ok bool, err error) {
	entries := []T{entry}
	_, query, arguments, buildErr := specifications.BuildUpdate[T](ctx, entries)
	if buildErr != nil {
		err = errors.Warning("sql: update failed").WithCause(buildErr)
		return
	}
	result, execErr := sql.Execute(ctx, query, arguments...)
	if execErr != nil {
		err = errors.Warning("sql: update failed").WithCause(execErr)
		return
	}
	if ok = result.RowsAffected == 1; ok {
		verErr := specifications.TrySetupAuditVersion[T](ctx, entries)
		if verErr != nil {
			err = errors.Warning("sql: update failed").WithCause(verErr)
			return
		}
		v = entries[0]
	}
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

func UpdateFields[T Table](ctx context.Context, fields FieldValues, cond conditions.Condition) (affected int64, err error) {
	_, query, arguments, buildErr := specifications.BuildUpdateFields[T](ctx, fields, specifications.Condition{Condition: cond})
	if buildErr != nil {
		err = errors.Warning("sql: update fields failed").WithCause(buildErr)
		return
	}
	result, execErr := sql.Execute(ctx, query, arguments...)
	if execErr != nil {
		err = errors.Warning("sql: update fields failed").WithCause(execErr)
		return
	}
	affected = result.RowsAffected
	return
}
