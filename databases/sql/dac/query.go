package dac

import (
	"github.com/aacfactory/errors"
	"github.com/aacfactory/fns-contrib/databases/sql"
	"github.com/aacfactory/fns-contrib/databases/sql/dac/conditions"
	"github.com/aacfactory/fns-contrib/databases/sql/dac/orders"
	"github.com/aacfactory/fns-contrib/databases/sql/dac/specifications"
	"github.com/aacfactory/fns/context"
)

type QueryOptions struct {
	cond   conditions.Condition
	orders orders.Orders
}

type QueryOption func(options *QueryOptions)

func Conditions(cond conditions.Condition) QueryOption {
	return func(options *QueryOptions) {
		options.cond = cond
	}
}

func Orders(orders orders.Orders) QueryOption {
	return func(options *QueryOptions) {
		options.orders = orders
	}
}

func Asc(name string) orders.Orders {
	return orders.Asc(name)
}

func Desc(name string) orders.Orders {
	return orders.Desc(name)
}

func Query[T Table](ctx context.Context, offset int, length int, options ...QueryOption) (entries []T, err error) {
	opt := QueryOptions{}
	for _, option := range options {
		option(&opt)
	}

	_, query, arguments, columns, buildErr := specifications.BuildQuery[T](
		ctx,
		specifications.Condition{Condition: opt.cond},
		specifications.Orders(opt.orders),
		offset, length,
	)
	if buildErr != nil {
		err = errors.Warning("sql: query failed").WithCause(buildErr)
		return
	}

	rows, queryErr := sql.Query(ctx, query, arguments...)
	if queryErr != nil {
		err = errors.Warning("sql: query failed").WithCause(queryErr)
		return
	}

	entries, err = specifications.ScanRows[T](ctx, rows, columns)
	_ = rows.Close()
	if err != nil {
		err = errors.Warning("sql: query failed").WithCause(err)
		return
	}
	return
}

func One[T Table](ctx context.Context, options ...QueryOption) (entry T, has bool, err error) {
	entries, queryErr := Query[T](ctx, 0, 1, options...)
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

func ALL[T Table](ctx context.Context, options ...QueryOption) (entries []T, err error) {
	entries, err = Query[T](ctx, 0, 0, options...)
	return
}
