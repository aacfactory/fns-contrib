package dac

import (
	"github.com/aacfactory/fns-contrib/databases/sql/dac/conditions"
	"github.com/aacfactory/fns/context"
)

func Delete[T Table](ctx context.Context, entry T) (err error) {

	return
}

func DeleteByCondition[T Table](ctx context.Context, cond conditions.Condition) (err error) {

	return
}
