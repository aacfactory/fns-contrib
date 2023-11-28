package dac

import (
	"github.com/aacfactory/fns-contrib/databases/sql/dac/specifications"
)

type QueryInterceptor interface {
	specifications.QueryInterceptor
}

type InsertInterceptor interface {
	specifications.InsertInterceptor
}

type UpdateInterceptor interface {
	specifications.UpdateInterceptor
}

type DeleteInterceptor interface {
	specifications.DeleteInterceptor
}
