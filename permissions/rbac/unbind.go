package rbac

import (
	"github.com/aacfactory/errors"
	"github.com/aacfactory/fns/context"
	"github.com/aacfactory/fns/runtime"
	"github.com/aacfactory/fns/services"
	"github.com/aacfactory/fns/services/authorizations"
)

var (
	unbindFnName = []byte("unbind")
)

type UnbindParam struct {
	Account authorizations.Id `json:"account"`
	Roles   Roles             `json:"roles"`
}

func Unbind(ctx context.Context, param UnbindParam) (err error) {
	eps := runtime.Endpoints(ctx)
	_, err = eps.Request(ctx, endpointName, unbindFnName, param)
	return
}

type unbindFn struct {
	store Store
}

func (fn *unbindFn) Name() string {
	return string(unbindFnName)
}

func (fn *unbindFn) Internal() bool {
	return false
}

func (fn *unbindFn) Readonly() bool {
	return false
}

func (fn *unbindFn) Handle(ctx services.Request) (v any, err error) {
	param, paramErr := services.ValueOfParam[UnbindParam](ctx.Param())
	if paramErr != nil {
		err = errors.Warning("rbac: unbind failed").WithCause(paramErr)
		return
	}
	err = fn.store.Unbind(ctx, param.Account, param.Roles)
	if err != nil {
		err = errors.Warning("rbac: unbind failed").WithCause(err)
		return
	}
	return
}
