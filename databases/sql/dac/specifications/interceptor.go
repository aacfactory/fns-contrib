package specifications

import "github.com/aacfactory/fns/context"

type QueryInterceptor interface {
	BeforeQuery(ctx context.Context) (err error)
}

type InsertInterceptor interface {
	BeforeInsert(ctx context.Context) (err error)
}

type UpdateInterceptor interface {
	BeforeUpdate(ctx context.Context) (err error)
}

type DeleteInterceptor interface {
	BeforeDelete(ctx context.Context) (err error)
}

func (spec *Specification) TryExecuteQueryInterceptor(ctx context.Context, instance Table) (err error) {
	if spec.queryInterceptor {
		ptr := any(&instance)
		hook, isHook := ptr.(QueryInterceptor)
		if isHook {
			err = hook.BeforeQuery(ctx)
			return
		}
	}
	return
}

func (spec *Specification) TryExecuteInsertInterceptor(ctx context.Context, instance Table) (err error) {
	if spec.insertInterceptor {
		ptr := any(&instance)
		hook, isHook := ptr.(InsertInterceptor)
		if isHook {
			err = hook.BeforeInsert(ctx)
			return
		}
	}
	return
}

func (spec *Specification) TryExecuteUpdateInterceptor(ctx context.Context, instance Table) (err error) {
	if spec.updateInterceptor {
		ptr := any(&instance)
		hook, isHook := ptr.(UpdateInterceptor)
		if isHook {
			err = hook.BeforeUpdate(ctx)
			return
		}
	}
	return
}

func (spec *Specification) TryExecuteDeleteInterceptor(ctx context.Context, instance Table) (err error) {
	if spec.deleteInterceptor {
		ptr := any(&instance)
		hook, isHook := ptr.(DeleteInterceptor)
		if isHook {
			err = hook.BeforeDelete(ctx)
			return
		}
	}
	return
}
