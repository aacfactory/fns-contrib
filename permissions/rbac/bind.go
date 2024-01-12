package rbac

import (
	"github.com/aacfactory/errors"
	"github.com/aacfactory/fns/context"
	"github.com/aacfactory/fns/runtime"
	"github.com/aacfactory/fns/services"
	"github.com/aacfactory/fns/services/authorizations"
	"github.com/aacfactory/fns/services/caches"
)

var (
	bindFnName = []byte("bind")
)

type BindParam struct {
	Account authorizations.Id `json:"account" avro:"account"`
	Roles   Roles             `json:"roles" avro:"roles"`
}

func Bind(ctx context.Context, param BindParam) (err error) {
	eps := runtime.Endpoints(ctx)
	_, err = eps.Request(ctx, endpointName, bindFnName, param)
	return
}

type bindFn struct {
	store     Store
	cacheable bool
}

func (fn *bindFn) Name() string {
	return string(bindFnName)
}

func (fn *bindFn) Internal() bool {
	return false
}

func (fn *bindFn) Readonly() bool {
	return false
}

func (fn *bindFn) Handle(ctx services.Request) (v any, err error) {
	param, paramErr := services.ValueOfParam[BindParam](ctx.Param())
	if paramErr != nil {
		err = errors.Warning("rbac: bind failed").WithCause(paramErr)
		return
	}
	err = fn.store.Bind(ctx, param.Account, param.Roles)
	if err != nil {
		err = errors.Warning("rbac: bind failed").WithCause(err)
		return
	}
	if fn.cacheable {
		_ = caches.Remove(ctx, CacheParam{Account: param.Account})
	}
	return
}
