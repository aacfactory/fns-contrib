package dac

import (
	"github.com/aacfactory/fns-contrib/databases/sql/dac/specifications"
)

type QueryHook interface {
	specifications.QueryHook
}

type InsertHook interface {
	specifications.InsertHook
}

type UpdateHook interface {
	specifications.UpdateHook
}

type DeleteHook interface {
	specifications.DeleteHook
}
