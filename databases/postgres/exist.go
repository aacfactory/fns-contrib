package postgres

import (
	"github.com/aacfactory/fns-contrib/databases/postgres/dialect"
	"github.com/aacfactory/fns-contrib/databases/sql"
	"github.com/aacfactory/fns-contrib/databases/sql/dac"
	"github.com/aacfactory/fns-contrib/databases/sql/dac/conditions"
	"github.com/aacfactory/fns/context"
)

func Exist[T Table](ctx context.Context, cond conditions.Condition) (has bool, err error) {
	sql.ForceDialect(ctx, dialect.Name)
	has, err = dac.Exist[T](ctx, cond)
	return
}
