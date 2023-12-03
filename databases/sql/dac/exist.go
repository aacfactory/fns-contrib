package dac

import (
	"github.com/aacfactory/errors"
	"github.com/aacfactory/fns-contrib/databases/sql"
	"github.com/aacfactory/fns-contrib/databases/sql/dac/conditions"
	"github.com/aacfactory/fns-contrib/databases/sql/dac/specifications"
	"github.com/aacfactory/fns/context"
)

func Exist[T Table](ctx context.Context, cond conditions.Condition) (has bool, err error) {
	_, query, arguments, buildErr := specifications.BuildExist[T](ctx, specifications.Condition{Condition: cond})
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
