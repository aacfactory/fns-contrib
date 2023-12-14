package rbac

import (
	"github.com/aacfactory/errors"
	"github.com/aacfactory/fns/context"
	"github.com/aacfactory/fns/runtime"
	"github.com/aacfactory/fns/services"
)

var (
	removeFnName = []byte("remove")
)

func Remove(ctx context.Context, id string, cascade bool) (v Role, err error) {
	_, rErr := runtime.Endpoints(ctx).Request(ctx, endpointName, removeFnName, removeParam{
		Id:      id,
		Cascade: cascade,
	})
	if rErr != nil {
		err = rErr
		return
	}
	return
}

type removeParam struct {
	Id      string `json:"id"`
	Cascade bool   `json:"cascade"`
}

type removeFn struct {
	store Store
}

func (fn *removeFn) Name() string {
	return string(removeFnName)
}

func (fn *removeFn) Internal() bool {
	return true
}

func (fn *removeFn) Readonly() bool {
	return false
}

func (fn *removeFn) Handle(ctx services.Request) (v any, err error) {
	param, paramErr := services.ValueOfParam[removeParam](ctx.Param())
	if paramErr != nil {
		err = errors.Warning("rbac: remove failed").WithCause(paramErr)
		return
	}
	role, has, roleErr := fn.store.Role(ctx, param.Id)
	if roleErr != nil {
		err = errors.Warning("rbac: remove failed").WithCause(roleErr)
		return
	}
	if !has {
		return
	}
	if !param.Cascade && len(role.Children) > 0 {
		err = ErrCantRemoveHasChildrenRow
		return
	}
	rmErr := fn.store.RemoveRole(ctx, role)
	if rmErr != nil {
		err = errors.Warning("rbac: remove failed").WithCause(rmErr)
		return
	}
	return
}
