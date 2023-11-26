package dac

import (
	"github.com/aacfactory/fns-contrib/databases/sql/dac/conditions"
	"github.com/aacfactory/fns-contrib/databases/sql/dac/selects"
)

type Options struct {
	cond   conditions.Condition
	orders selects.Orders
}

type Option func(options *Options)

func Cond(cond conditions.Condition) Option {
	return func(options *Options) {
		options.cond = cond
	}
}

func Orders(orders selects.Orders) Option {
	return func(options *Options) {
		options.orders = orders
	}
}

func Asc(name string) selects.Orders {
	return selects.Asc(name)
}

func Desc(name string) selects.Orders {
	return selects.Desc(name)
}
