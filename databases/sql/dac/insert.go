package dac

import (
	"github.com/aacfactory/fns-contrib/databases/sql/dac/conditions"
	"github.com/aacfactory/fns/context"
)

type InsertOptions struct {
	saveMode bool
	notExist conditions.QueryExpr
	exist    conditions.QueryExpr
}

type InsertOption func(options *InsertOptions)

func InsertOrUpdate() InsertOption {
	return func(options *InsertOptions) {
		options.saveMode = true
	}
}

func InsertWhenExist(src conditions.QueryExpr) InsertOption {
	return func(options *InsertOptions) {
		options.exist = src
	}
}

func InsertWhenNotExist(src conditions.QueryExpr) InsertOption {
	return func(options *InsertOptions) {
		options.notExist = src
	}
}

func Insert[T Table](ctx context.Context, entry T, options ...InsertOption) (v T, err error) {

	return
}
