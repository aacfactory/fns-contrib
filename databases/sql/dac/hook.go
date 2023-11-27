package dac

import "github.com/aacfactory/fns/context"

type QueryHook interface {
	AfterQuery(ctx context.Context) (err error)
}

type InsertHook interface {
	AfterInsert(ctx context.Context) (err error)
}

type UpdateHook interface {
	AfterUpdate(ctx context.Context) (err error)
}

type DeleteHook interface {
	AfterDelete(ctx context.Context) (err error)
}
