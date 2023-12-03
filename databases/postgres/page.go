package postgres

import (
	"github.com/aacfactory/fns-contrib/databases/postgres/dialect"
	"github.com/aacfactory/fns-contrib/databases/sql"
	"github.com/aacfactory/fns-contrib/databases/sql/dac"
	"github.com/aacfactory/fns/context"
)

func Page[T Table](ctx context.Context, no int, size int, options ...QueryOption) (page dac.Pager[T], err error) {
	sql.ForceDialect(ctx, dialect.Name)
	opts := acquireQueryOptions()
	for _, option := range options {
		opts = append(opts, dac.QueryOption(option))
	}
	page, err = dac.Page[T](ctx, no, size, opts...)
	releaseQueryOptions(opts)
	return
}
