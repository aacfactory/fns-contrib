package dac

import (
	"github.com/aacfactory/errors"
	"github.com/aacfactory/fns-contrib/databases/sql"
	"github.com/aacfactory/fns-contrib/databases/sql/dac/specifications"
	"github.com/aacfactory/fns/context"
)

func Views[V View](ctx context.Context, offset int, length int, options ...QueryOption) (entries []V, err error) {
	opt := QueryOptions{}
	for _, option := range options {
		option(&opt)
	}

	_, query, arguments, columns, buildErr := specifications.BuildView[V](
		ctx,
		specifications.Condition{Condition: opt.cond},
		specifications.Orders(opt.orders),
		specifications.GroupBy{GroupBy: opt.groupBy},
		offset, length,
	)
	if buildErr != nil {
		err = errors.Warning("sql: view failed").WithCause(buildErr)
		return
	}

	rows, queryErr := sql.Query(ctx, query, arguments...)
	if queryErr != nil {
		err = errors.Warning("sql: view failed").WithCause(queryErr)
		return
	}

	entries, err = specifications.ScanRows[V](ctx, rows, columns)
	_ = rows.Close()
	if err != nil {
		err = errors.Warning("sql: view failed").WithCause(err)
		return
	}
	return
}

func ViewOne[V View](ctx context.Context, options ...QueryOption) (entry V, has bool, err error) {
	entries, queryErr := Views[V](ctx, 0, 1, options...)
	if queryErr != nil {
		err = queryErr
		return
	}
	has = len(entries) == 1
	if has {
		entry = entries[0]
	}
	return
}

func ViewALL[V View](ctx context.Context, options ...QueryOption) (entries []V, err error) {
	entries, err = Views[V](ctx, 0, 0, options...)
	return
}
