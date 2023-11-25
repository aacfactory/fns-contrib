package dal

import "github.com/aacfactory/fns/context"

var (
	selectsCtxKey = []byte("@fns:sql:dal:selects")
)

func DefineSelectColumns(ctx context.Context, columns ...string) context.Context {
	ctx.SetLocalValue(selectsCtxKey, columns)
	return ctx
}

func DefinedSelectColumns(ctx context.Context) (columns []string, has bool) {
	columns, has, _ = context.LocalValue[[]string](ctx, selectsCtxKey)
	return
}
