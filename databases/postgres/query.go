package postgres

import (
	"github.com/aacfactory/fns-contrib/databases/postgres/dialect"
	"github.com/aacfactory/fns-contrib/databases/sql"
	"github.com/aacfactory/fns-contrib/databases/sql/dac"
	"github.com/aacfactory/fns-contrib/databases/sql/dac/conditions"
	"github.com/aacfactory/fns-contrib/databases/sql/dac/groups"
	"github.com/aacfactory/fns-contrib/databases/sql/dac/orders"
	"github.com/aacfactory/fns/context"
	"sync"
)

type QueryOptions struct {
	dac.QueryOption
}

type QueryOption dac.QueryOption

func Conditions(cond conditions.Condition) QueryOption {
	return QueryOption(dac.Conditions(cond))
}

func Orders(order orders.Orders) QueryOption {
	return QueryOption(dac.Orders(order))
}

func Asc(name string) orders.Orders {
	return orders.Asc(name)
}

func Desc(name string) orders.Orders {
	return orders.Desc(name)
}

func GroupBy(by groups.GroupBy) QueryOption {
	return QueryOption(dac.GroupBy(by))
}

var (
	queryOptionsPool = sync.Pool{New: func() any {
		return make([]dac.QueryOption, 0, 1)
	}}
)

func acquireQueryOptions() []dac.QueryOption {
	return queryOptionsPool.Get().([]dac.QueryOption)
}

func releaseQueryOptions(options []dac.QueryOption) {
	queryOptionsPool.Put(options[:0])
}

func Query[T Table](ctx context.Context, offset int, length int, options ...QueryOption) (entries []T, err error) {
	sql.ForceDialect(ctx, dialect.Name)
	opts := acquireQueryOptions()
	for _, option := range options {
		opts = append(opts, dac.QueryOption(option))
	}
	entries, err = dac.Query[T](ctx, offset, length, opts...)
	releaseQueryOptions(opts)
	return
}

func One[T Table](ctx context.Context, options ...QueryOption) (entry T, has bool, err error) {
	sql.ForceDialect(ctx, dialect.Name)
	opts := acquireQueryOptions()
	for _, option := range options {
		opts = append(opts, dac.QueryOption(option))
	}
	entry, has, err = dac.One[T](ctx, opts...)
	releaseQueryOptions(opts)
	return
}

func ALL[T Table](ctx context.Context, options ...QueryOption) (entries []T, err error) {
	sql.ForceDialect(ctx, dialect.Name)
	opts := acquireQueryOptions()
	for _, option := range options {
		opts = append(opts, dac.QueryOption(option))
	}
	entries, err = dac.ALL[T](ctx, opts...)
	releaseQueryOptions(opts)
	return
}
