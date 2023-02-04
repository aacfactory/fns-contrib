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

func QueryTree[T TreeModel, N keyable](ctx context.Context, conditions *Conditions, rootNodeValue N) (result T, err errors.CodeError) {
	results, queryErr := queryTrees[T, N](ctx, conditions, rootNodeValue)
	if queryErr != nil {
		err = errors.ServiceError("dal: query tree failed").WithCause(queryErr)
		return
	}
	if results == nil || len(results) == 0 {
		return
	}
	result = results[rootNodeValue]
	return
}

func QueryTrees[T TreeModel, N keyable](ctx context.Context, conditions *Conditions, rootNodeValues ...N) (results map[N]T, err errors.CodeError) {
	results, err = queryTrees[T, N](ctx, conditions, rootNodeValues...)
	if err != nil {
		err = errors.ServiceError("dal: query trees failed").WithCause(err)
		return
	}
	return
}

func queryTrees[T TreeModel, N keyable](ctx context.Context, conditions *Conditions, rootNodeValues ...N) (results map[N]T, err errors.CodeError) {
	ctx = NotEagerLoad(ctx)
	// todo query

	// todo map list to trees
	return
}
