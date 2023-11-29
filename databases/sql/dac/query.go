package dac

import (
	"github.com/aacfactory/errors"
	"github.com/aacfactory/fns-contrib/databases/sql"
	"github.com/aacfactory/fns-contrib/databases/sql/dac/conditions"
	"github.com/aacfactory/fns-contrib/databases/sql/dac/specifications"
	"github.com/aacfactory/fns/context"
)

type QueryOptions struct {
	cond     conditions.Condition
	orders   specifications.Orders
	groupBys specifications.GroupBy
	having   specifications.Having
}

type QueryOption func(options *QueryOptions)

func Conditions(cond conditions.Condition) QueryOption {
	return func(options *QueryOptions) {
		options.cond = cond
	}
}

func Orders(orders specifications.Orders) QueryOption {
	return func(options *QueryOptions) {
		options.orders = orders
	}
}

func Asc(name string) specifications.Orders {
	return specifications.Asc(name)
}

func Desc(name string) specifications.Orders {
	return specifications.Desc(name)
}

func GroupBy(fields ...string) QueryOption {
	return func(options *QueryOptions) {
		options.groupBys = specifications.NewGroupBy(fields...)
	}
}

func Having(cond conditions.Condition) QueryOption {
	return func(options *QueryOptions) {
		options.having = specifications.NewHaving(cond)
	}
}

func Query[T Table](ctx context.Context, offset int, length int, options ...QueryOption) (entries []T, err error) {
	dialect, dialectErr := specifications.LoadDialect(ctx)
	if dialectErr != nil {
		err = errors.Warning("sql: query failed").WithCause(dialectErr)
		return
	}
	t := specifications.TableInstance[T]()
	spec, specErr := specifications.GetSpecification(ctx, t)
	if specErr != nil {
		err = errors.Warning("sql: query failed").WithCause(specErr)
		return
	}

	opt := QueryOptions{}
	for _, option := range options {
		option(&opt)
	}

	_, query, arguments, columns, buildErr := dialect.Query(
		specifications.Todo(ctx, t, dialect),
		spec,
		specifications.Condition{Condition: opt.cond},
		opt.orders,
		opt.groupBys,
		opt.having,
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
	dialect, dialectErr := specifications.LoadDialect(ctx)
	if dialectErr != nil {
		err = errors.Warning("sql: query one failed").WithCause(dialectErr)
		return
	}
	spec, specErr := specifications.GetSpecification(ctx, entry)
	if specErr != nil {
		err = errors.Warning("sql: query one failed").WithCause(specErr)
		return
	}

	opt := QueryOptions{}
	for _, option := range options {
		option(&opt)
	}

	_, query, arguments, columns, buildErr := dialect.Query(
		specifications.Todo(ctx, entry, dialect),
		spec,
		specifications.Condition{Condition: opt.cond},
		opt.orders,
		opt.groupBys,
		opt.having,
		0, 1,
	)
	if buildErr != nil {
		err = errors.Warning("sql: query one failed").WithCause(buildErr)
		return
	}

	rows, queryErr := sql.Query(ctx, query, arguments...)
	if queryErr != nil {
		err = errors.Warning("sql: query one failed").WithCause(queryErr)
		return
	}

	entries, scanErr := specifications.ScanRows[T](ctx, rows, columns)
	_ = rows.Close()
	if scanErr != nil {
		err = errors.Warning("sql: query one failed").WithCause(scanErr)
		return
	}
	has = len(entries) > 0
	if has {
		entry = entries[0]
	}
	return
}
