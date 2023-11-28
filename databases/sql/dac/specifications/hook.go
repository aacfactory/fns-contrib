package specifications

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

func (spec *Specification) TryExecuteQueryHook(ctx context.Context, instance Table) (err error) {
	if spec.queryHook {
		ptr := any(&instance)
		hook, isHook := ptr.(QueryHook)
		if isHook {
			err = hook.AfterQuery(ctx)
			return
		}
	}
	return
}

func (spec *Specification) TryExecuteInsertHook(ctx context.Context, instance Table) (err error) {
	if spec.insertHook {
		ptr := any(&instance)
		hook, isHook := ptr.(InsertHook)
		if isHook {
			err = hook.AfterInsert(ctx)
			return
		}
	}
	return
}

func (spec *Specification) TryExecuteUpdateHook(ctx context.Context, instance Table) (err error) {
	if spec.updateHook {
		ptr := any(&instance)
		hook, isHook := ptr.(UpdateHook)
		if isHook {
			err = hook.AfterUpdate(ctx)
			return
		}
	}
	return
}

func (spec *Specification) TryExecuteDeleteHook(ctx context.Context, instance Table) (err error) {
	if spec.deleteHook {
		ptr := any(&instance)
		hook, isHook := ptr.(DeleteHook)
		if isHook {
			err = hook.AfterDelete(ctx)
			return
		}
	}
	return
}
