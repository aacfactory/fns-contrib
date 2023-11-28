package dac

import (
	"github.com/aacfactory/fns-contrib/databases/sql"
	"github.com/aacfactory/fns/context"
)

func ForceDialect(ctx context.Context, dialect string) context.Context {
	return sql.ForceDialect(ctx, dialect)
}

func Dialect(ctx context.Context) (dialect string, err error) {
	return sql.Dialect(ctx)
}
