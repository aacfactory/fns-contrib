package rbac

import (
	"github.com/aacfactory/errors"
	"github.com/aacfactory/fns/context"
	"github.com/aacfactory/fns/runtime"
	"github.com/aacfactory/fns/services"
)

var (
	saveFnName = []byte("save")
)

func Save(ctx context.Context, role Role) (err error) {
	_, rErr := runtime.Endpoints(ctx).Request(ctx, endpointName, saveFnName, role)
	if rErr != nil {
		err = rErr
		return
	}
	return
}

type saveFn struct {
	store Store
}

func (fn *saveFn) Name() string {
	return string(saveFnName)
}

func (fn *saveFn) Internal() bool {
	return true
}

func (fn *saveFn) Readonly() bool {
	return false
}

func (fn *saveFn) Handle(ctx services.Request) (v any, err error) {
	role, paramErr := services.ValueOfParam[Role](ctx.Param())
	if paramErr != nil {
		err = errors.Warning("rbac: save failed").WithCause(paramErr)
		return
	}
	saveErr := fn.store.SaveRole(ctx, role)
	if saveErr != nil {
		err = errors.Warning("rbac: save failed").WithCause(saveErr)
		return
	}
	return
}
