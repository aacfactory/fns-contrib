package dac

import (
	"github.com/aacfactory/errors"
	"github.com/aacfactory/fns-contrib/databases/sql"
	"github.com/aacfactory/fns-contrib/databases/sql/dac/conditions"
	"github.com/aacfactory/fns-contrib/databases/sql/dac/specifications"
	"github.com/aacfactory/fns/context"
)

func Count[T Table](ctx context.Context, cond conditions.Condition) (count int64, err error) {
	dialect, dialectErr := specifications.LoadDialect(ctx)
	if dialectErr != nil {
		err = errors.Warning("sql: count failed").WithCause(dialectErr)
		return
	}
	t := specifications.ZeroInstance[T]()
	spec, specErr := specifications.GetSpecification(ctx, t)
	if specErr != nil {
		err = errors.Warning("sql: count failed").WithCause(specErr)
		return
	}

	_, query, arguments, buildErr := dialect.Count(specifications.Todo(ctx, t, dialect), spec, specifications.Condition{Condition: cond})
	if buildErr != nil {
		err = errors.Warning("sql: count failed").WithCause(buildErr)
		return
	}

	rows, queryErr := sql.Query(ctx, query, arguments...)
	if queryErr != nil {
		err = errors.Warning("sql: count failed").WithCause(queryErr)
		return
	}

	if rows.Next() {
		scanErr := rows.Scan(&count)
		if scanErr != nil {
			_ = rows.Close()
			err = errors.Warning("sql: count failed").WithCause(queryErr)
			return
		}
	}
	_ = rows.Close()
	return
}
