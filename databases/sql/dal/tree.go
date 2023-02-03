package dal

import (
	"context"
	"github.com/aacfactory/errors"
)

type keyable interface {
	~int | ~int8 | ~int16 | ~int32 | ~int64 |
		~uint | ~uint8 | ~uint16 | ~uint32 | ~uint64 | ~uintptr |
		~float32 | ~float64 | ~string
}

func QueryTree[T TreeModel, N keyable](ctx context.Context, conditions *Conditions, rootNodeValues ...N) (result T, err errors.CodeError) {
	ctx = NotEagerLoad(ctx)
	// todo query

	// todo map list to trees
	return
}

func QueryTrees[T TreeModel, N keyable](ctx context.Context, conditions *Conditions, rootNodeValues ...N) (results map[N]T, err errors.CodeError) {
	ctx = NotEagerLoad(ctx)
	// todo query

	// todo map list to trees
	return
}
