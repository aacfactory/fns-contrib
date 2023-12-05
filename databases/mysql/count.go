package mysql

import (
	"github.com/aacfactory/fns-contrib/databases/mysql/dialect"
	"github.com/aacfactory/fns-contrib/databases/sql"
	"github.com/aacfactory/fns-contrib/databases/sql/dac"
	"github.com/aacfactory/fns-contrib/databases/sql/dac/conditions"
	"github.com/aacfactory/fns/context"
)

func Count[T Table](ctx context.Context, cond conditions.Condition) (count int64, err error) {
	sql.ForceDialect(ctx, dialect.Name)
	count, err = dac.Count[T](ctx, cond)
	return
}
