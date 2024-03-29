package dac

import (
	"github.com/aacfactory/fns-contrib/databases/sql"
	"github.com/aacfactory/fns/context"
)

func Use(ctx context.Context, endpointName []byte) context.Context {
	return sql.Use(ctx, endpointName)
}

func Disuse(ctx context.Context) context.Context {
	return sql.Disuse(ctx)
}
