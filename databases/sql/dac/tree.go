package dac

import (
	"github.com/aacfactory/errors"
	"github.com/aacfactory/fns-contrib/databases/sql"
	"github.com/aacfactory/fns-contrib/databases/sql/dac/specifications"
	"github.com/aacfactory/fns/commons/container/trees"
	"github.com/aacfactory/fns/context"
)

func Tree[T Table](ctx context.Context, options ...QueryOption) (entry T, err error) {
	entries, entriesErr := Trees[T](ctx, options...)
	if entriesErr != nil {
		err = entriesErr
		return
	}
	if len(entries) > 0 {
		entry = entries[0]
	}
	return
}

func Trees[T Table](ctx context.Context, options ...QueryOption) (entries []T, err error) {
	opt := QueryOptions{}
	for _, option := range options {
		option(&opt)
	}

	_, query, arguments, columns, buildErr := specifications.BuildQuery[T](
		ctx,
		specifications.Condition{Condition: opt.cond},
		specifications.Orders(opt.orders),
		specifications.GroupBy(opt.groupBys),
		specifications.Having{HavingCondition: opt.having},
		0, 0,
	)
	if buildErr != nil {
		err = errors.Warning("sql: tree failed").WithCause(buildErr)
		return
	}

	rows, queryErr := sql.Query(ctx, query, arguments...)
	if queryErr != nil {
		err = errors.Warning("sql: tree failed").WithCause(queryErr)
		return
	}

	entries, err = specifications.ScanRows[T](ctx, rows, columns)
	_ = rows.Close()
	if err != nil {
		err = errors.Warning("sql: tree failed").WithCause(err)
		return
	}
	entries, err = trees.ConvertListToTree[T](entries)
	if err != nil {
		err = errors.Warning("sql: tree failed").WithCause(err)
		return
	}
	return
}
