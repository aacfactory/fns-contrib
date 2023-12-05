package mysql

import (
	"github.com/aacfactory/fns-contrib/databases/mysql/dialect"
	"github.com/aacfactory/fns-contrib/databases/sql"
	"github.com/aacfactory/fns-contrib/databases/sql/dac"
	"github.com/aacfactory/fns/context"
)

func Views[V View](ctx context.Context, offset int, length int, options ...QueryOption) (entries []V, err error) {
	sql.ForceDialect(ctx, dialect.Name)
	opts := acquireQueryOptions()
	for _, option := range options {
		opts = append(opts, dac.QueryOption(option))
	}
	entries, err = dac.Views[V](ctx, offset, length, opts...)
	releaseQueryOptions(opts)
	return
}

func ViewOne[V View](ctx context.Context, options ...QueryOption) (entry V, has bool, err error) {
	sql.ForceDialect(ctx, dialect.Name)
	opts := acquireQueryOptions()
	for _, option := range options {
		opts = append(opts, dac.QueryOption(option))
	}
	entry, has, err = dac.ViewOne[V](ctx, opts...)
	releaseQueryOptions(opts)
	return
}

func ViewALL[V View](ctx context.Context, options ...QueryOption) (entries []V, err error) {
	sql.ForceDialect(ctx, dialect.Name)
	opts := acquireQueryOptions()
	for _, option := range options {
		opts = append(opts, dac.QueryOption(option))
	}
	entries, err = dac.ViewALL[V](ctx, opts...)
	releaseQueryOptions(opts)
	return
}
