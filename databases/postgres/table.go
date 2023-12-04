package postgres

import "github.com/aacfactory/fns-contrib/databases/sql/dac"

type Table interface {
	dac.Table
}

type View interface {
	dac.View
}
