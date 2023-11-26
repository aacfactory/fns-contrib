package dac

import (
	"github.com/aacfactory/fns-contrib/databases/sql/dac/models"
	"github.com/aacfactory/fns/context"
)

func Query[T models.Table](ctx context.Context, offset int, length int, options ...Option) (entries []T, err error) {

	return
}

func One[T models.Table](ctx context.Context, options ...Option) (entry T, err error) {

	return
}
