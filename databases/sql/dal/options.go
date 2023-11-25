package dal

import (
	"github.com/aacfactory/fns-contrib/databases/sql"
	"github.com/aacfactory/fns/context"
)

var (
	eagerLoadCtxKey = []byte("@fns:sql:dal:load_eager")
)

func Endpoint(ctx context.Context, name []byte) context.Context {
	sql.EndpointName(ctx, name)
	return ctx
}

func EagerLoad(ctx context.Context) context.Context {
	ctx.SetLocalValue(eagerLoadCtxKey, true)
	return ctx
}

func NotEagerLoad(ctx context.Context) context.Context {
	ctx.SetLocalValue(eagerLoadCtxKey, false)
	return ctx
}

func isEagerLoadMode(ctx context.Context) (ok bool) {
	ok, _, _ = context.LocalValue[bool](ctx, eagerLoadCtxKey)
	return
}
