package rbac

import (
	"github.com/aacfactory/errors"
	"github.com/aacfactory/fns/context"
	"github.com/aacfactory/fns/runtime"
	"github.com/aacfactory/fns/services"
)

var (
	getFnName = []byte("get")
)

func Get(ctx context.Context, id string) (v Role, err error) {
	r, rErr := runtime.Endpoints(ctx).Request(ctx, endpointName, getFnName, id)
	if rErr != nil {
		err = rErr
		return
	}
	v, err = services.ValueOfResponse[Role](r)
	if err != nil {
		err = errors.Warning("rbac: get failed").WithCause(err)
		return
	}
	return
}

type getFn struct {
	store Store
}

func (fn *getFn) Name() string {
	return string(getFnName)
}

func (fn *getFn) Internal() bool {
	return true
}

func (fn *getFn) Readonly() bool {
	return false
}

func (fn *getFn) Handle(ctx services.Request) (v any, err error) {
	id, paramErr := services.ValueOfParam[string](ctx.Param())
	if paramErr != nil {
		err = errors.Warning("rbac: get failed").WithCause(paramErr)
		return
	}
	role, has, roleErr := fn.store.Role(ctx, id)
	if roleErr != nil {
		err = errors.Warning("rbac: get failed").WithCause(roleErr)
		return
	}
	if !has {
		err = ErrRoleNofFound
		return
	}
	v = role
	return
}
