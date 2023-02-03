package dal

import (
	"context"
	"github.com/aacfactory/fns-contrib/databases/sql"
)

func WithDatabase(ctx context.Context, database string) context.Context {
	return sql.WithOptions(ctx, sql.Database(database))
}
