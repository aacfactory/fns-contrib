package dac

import (
	"github.com/aacfactory/errors"
	"github.com/aacfactory/fns-contrib/databases/sql"
	"github.com/aacfactory/fns-contrib/databases/sql/dac/conditions"
	"github.com/aacfactory/fns-contrib/databases/sql/dac/specifications"
	"github.com/aacfactory/fns/context"
)

func Exist[T Table](ctx context.Context, cond conditions.Condition) (has bool, err error) {
	dialect, dialectErr := specifications.LoadDialect(ctx)
	if dialectErr != nil {
		err = errors.Warning("sql: exist failed").WithCause(dialectErr)
		return
	}
	t := specifications.ZeroInstance[T]()
	spec, specErr := specifications.GetSpecification(ctx, t)
	if specErr != nil {
		err = errors.Warning("sql: exist failed").WithCause(specErr)
		return
	}

	_, query, arguments, buildErr := dialect.Exist(specifications.Todo(ctx, t, dialect), spec, specifications.Condition{Condition: cond})
	if buildErr != nil {
		err = errors.Warning("sql: exist failed").WithCause(buildErr)
		return
	}

	rows, queryErr := sql.Query(ctx, query, arguments...)
	if queryErr != nil {
		err = errors.Warning("sql: exist failed").WithCause(queryErr)
		return
	}
	has = rows.Next()
	_ = rows.Close()
	return
}
