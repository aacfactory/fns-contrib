package postgres

import (
	"github.com/aacfactory/fns-contrib/databases/postgres/dialect"
	"github.com/aacfactory/fns-contrib/databases/sql"
	"github.com/aacfactory/fns-contrib/databases/sql/dac"
	"github.com/aacfactory/fns-contrib/databases/sql/dac/conditions"
	"github.com/aacfactory/fns/context"
)

func Delete[T Table](ctx context.Context, entry T) (v T, affected int64, err error) {
	sql.ForceDialect(ctx, dialect.Name)
	v, affected, err = dac.Delete[T](ctx, entry)
	return
}

func DeleteByCondition[T Table](ctx context.Context, cond conditions.Condition) (affected int64, err error) {
	sql.ForceDialect(ctx, dialect.Name)
	affected, err = dac.DeleteByCondition[T](ctx, cond)
	return
}
