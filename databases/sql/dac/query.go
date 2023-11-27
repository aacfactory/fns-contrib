package dac

import (
	"github.com/aacfactory/fns-contrib/databases/sql/dac/conditions"
	"github.com/aacfactory/fns-contrib/databases/sql/dac/specifications"
	"github.com/aacfactory/fns/context"
)

type QueryOptions struct {
	cond   conditions.Condition
	orders specifications.Orders
}

type QueryOption func(options *QueryOptions)

func Conditions(cond conditions.Condition) QueryOption {
	return func(options *QueryOptions) {
		options.cond = cond
	}
}

func Orders(orders specifications.Orders) QueryOption {
	return func(options *QueryOptions) {
		options.orders = orders
	}
}

func Asc(name string) specifications.Orders {
	return specifications.Asc(name)
}

func Desc(name string) specifications.Orders {
	return specifications.Desc(name)
}

func Query[T Table](ctx context.Context, offset int, length int, options ...QueryOption) (entries []T, err error) {

	return
}

func One[T Table](ctx context.Context, options ...QueryOption) (entry T, err error) {

	return
}
