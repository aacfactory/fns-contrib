package dal

import (
	"context"
	"github.com/aacfactory/errors"
)

func QueryOne[T Model](ctx context.Context, conditions *Conditions) (result T, err errors.CodeError) {

	return
}

func Query[T Model](ctx context.Context, conditions *Conditions) (results []T, err errors.CodeError) {

	return
}

func QueryWithRange[T Model](ctx context.Context, conditions *Conditions, orders *Orders, rng *Range) (results []T, err errors.CodeError) {

	return
}

func query[T Model](ctx context.Context, conditions *Conditions, orders *Orders, rng *Range) (results []T, err errors.CodeError) {

	return
}
