package dal

import (
	"context"
	"github.com/aacfactory/fns-contrib/databases/sql"
)

const (
	eagerLoadCtxKey = "@fns_sql_dal_load_eager"
)

func Database(ctx context.Context, database string) context.Context {
	return sql.WithOptions(ctx, sql.Database(database))
}

func EagerLoad(ctx context.Context) context.Context {
	return context.WithValue(ctx, eagerLoadCtxKey, true)
}

func NotEagerLoad(ctx context.Context) context.Context {
	return context.WithValue(ctx, eagerLoadCtxKey, false)
}

func isEagerLoadMode(ctx context.Context) (ok bool) {
	v := ctx.Value(eagerLoadCtxKey)
	if v == nil {
		return
	}
	ok = v.(bool)
	return
}
