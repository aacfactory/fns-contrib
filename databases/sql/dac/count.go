package dac

import (
	"github.com/aacfactory/errors"
	"github.com/aacfactory/fns-contrib/databases/sql"
	"github.com/aacfactory/fns-contrib/databases/sql/dac/conditions"
	"github.com/aacfactory/fns-contrib/databases/sql/dac/specifications"
	"github.com/aacfactory/fns/context"
)

func Count[T Table](ctx context.Context, cond conditions.Condition) (count int64, err error) {
	_, query, arguments, buildErr := specifications.BuildCount[T](ctx, specifications.Condition{Condition: cond})
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
