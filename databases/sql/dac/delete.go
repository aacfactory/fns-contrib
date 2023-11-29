package dac

import (
	"github.com/aacfactory/errors"
	"github.com/aacfactory/fns-contrib/databases/sql"
	"github.com/aacfactory/fns-contrib/databases/sql/dac/conditions"
	"github.com/aacfactory/fns-contrib/databases/sql/dac/specifications"
	"github.com/aacfactory/fns/context"
)

func Delete[T Table](ctx context.Context, entry T) (err error) {
	dialect, dialectErr := specifications.LoadDialect(ctx)
	if dialectErr != nil {
		err = errors.Warning("sql: delete failed").WithCause(dialectErr)
		return
	}
	spec, specErr := specifications.GetSpecification(ctx, entry)
	if specErr != nil {
		err = errors.Warning("sql: delete failed").WithCause(specErr)
		return
	}

	_, query, arguments, buildErr := dialect.Delete(specifications.Todo(ctx, entry, dialect), spec, entry)
	if buildErr != nil {
		err = errors.Warning("sql: delete failed").WithCause(buildErr)
		return
	}

	result, execErr := sql.Execute(ctx, query, arguments...)
	if execErr != nil {
		err = errors.Warning("sql: delete failed").WithCause(execErr)
		return
	}
	if result.RowsAffected == 0 {
		err = ErrNoAffected
		return
	}
	return
}

func DeleteByCondition[T Table](ctx context.Context, cond conditions.Condition) (err error) {
	dialect, dialectErr := specifications.LoadDialect(ctx)
	if dialectErr != nil {
		err = errors.Warning("sql: delete failed").WithCause(dialectErr)
		return
	}
	t := specifications.TableInstance[T]()
	spec, specErr := specifications.GetSpecification(ctx, t)
	if specErr != nil {
		err = errors.Warning("sql: delete failed").WithCause(specErr)
		return
	}

	_, query, arguments, buildErr := dialect.DeleteByConditions(specifications.Todo(ctx, t, dialect), spec, specifications.Condition{Condition: cond})
	if buildErr != nil {
		err = errors.Warning("sql: delete failed").WithCause(buildErr)
		return
	}

	result, execErr := sql.Execute(ctx, query, arguments...)
	if execErr != nil {
		err = errors.Warning("sql: delete failed").WithCause(execErr)
		return
	}
	if result.RowsAffected == 0 {
		err = ErrNoAffected
		return
	}

	return
}
