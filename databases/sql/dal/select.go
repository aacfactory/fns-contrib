package dal

import "context"

const (
	selectsCtxKey = "@fns_sql_dal_selects"
)

func DefineSelectColumns(ctx context.Context, columns ...string) context.Context {
	return context.WithValue(ctx, selectsCtxKey, columns)
}

func DefinedSelectColumns(ctx context.Context) (columns []string, has bool) {
	v := ctx.Value(selectsCtxKey)
	if v == nil {
		return
	}
	columns, has = v.([]string)
	if has {
		has = len(columns) > 0
	}
	return
}
