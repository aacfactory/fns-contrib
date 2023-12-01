package dac

import (
	"github.com/aacfactory/errors"
	"github.com/aacfactory/fns/commons/container/trees"
	"github.com/aacfactory/fns/context"
)

func Tree[T Table](ctx context.Context, options ...QueryOption) (entry T, err error) {
	entries, entriesErr := Trees[T](ctx, options...)
	if entriesErr != nil {
		err = entriesErr
		return
	}
	if len(entries) > 0 {
		entry = entries[0]
	}
	return
}

func Trees[T Table](ctx context.Context, options ...QueryOption) (entries []T, err error) {
	entries, err = Query[T](ctx, 0, 0, options...)
	if err != nil {
		err = errors.Warning("sql: tree failed").WithCause(err)
		return
	}
	entries, err = trees.ConvertListToTree[T](entries)
	if err != nil {
		err = errors.Warning("sql: tree failed").WithCause(err)
		return
	}
	return
}
