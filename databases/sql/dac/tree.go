package dac

import (
	"github.com/aacfactory/fns-contrib/databases/sql/dac/conditions"
	"github.com/aacfactory/fns/context"
)

func Tree[T Table](ctx context.Context, field string, value any) (entry T, err error) {

	return
}

func Trees[T Table](ctx context.Context, cond conditions.Condition) (entries []T, err error) {

	return
}
