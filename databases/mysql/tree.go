package mysql

import (
	"github.com/aacfactory/fns-contrib/databases/mysql/dialect"
	"github.com/aacfactory/fns-contrib/databases/sql"
	"github.com/aacfactory/fns-contrib/databases/sql/dac"
	"github.com/aacfactory/fns/context"
)

func Tree[T Table](ctx context.Context, options ...QueryOption) (entry T, err error) {
	sql.ForceDialect(ctx, dialect.Name)
	opts := acquireQueryOptions()
	for _, option := range options {
		opts = append(opts, dac.QueryOption(option))
	}
	entry, err = dac.Tree[T](ctx, opts...)
	releaseQueryOptions(opts)
	return
}

func Trees[T Table](ctx context.Context, options ...QueryOption) (entries []T, err error) {
	sql.ForceDialect(ctx, dialect.Name)
	opts := acquireQueryOptions()
	for _, option := range options {
		opts = append(opts, dac.QueryOption(option))
	}
	entries, err = dac.Trees[T](ctx, opts...)
	releaseQueryOptions(opts)
	return
}
