package rbac

import (
	"github.com/aacfactory/errors"
	"github.com/aacfactory/fns/context"
	"github.com/aacfactory/fns/runtime"
	"github.com/aacfactory/fns/services"
	"github.com/aacfactory/fns/services/authorizations"
	"github.com/aacfactory/fns/services/caches"
	"time"
)

var (
	boundsFnName = []byte("bounds")
)

func Bounds(ctx context.Context, account authorizations.Id) (v Roles, err error) {
	r, rErr := runtime.Endpoints(ctx).Request(ctx, endpointName, boundsFnName, account)
	if rErr != nil {
		err = rErr
		return
	}
	v, err = services.ValueOfResponse[Roles](r)
	if err != nil {
		err = errors.Warning("rbac: bounds failed").WithCause(err)
		return
	}
	return
}

type boundsFn struct {
	store     Store
	cacheable bool
	cacheTTL  time.Duration
}

func (fn *boundsFn) Name() string {
	return string(boundsFnName)
}

func (fn *boundsFn) Internal() bool {
	return true
}

func (fn *boundsFn) Readonly() bool {
	return false
}

func (fn *boundsFn) Handle(ctx services.Request) (v any, err error) {
	account, paramErr := services.ValueOfParam[authorizations.Id](ctx.Param())
	if paramErr != nil {
		err = errors.Warning("rbac: bounds failed").WithCause(paramErr)
		return
	}
	if fn.cacheable {
		roles := make(Roles, 0)
		cached, _ := caches.Load(ctx, CacheParam{Account: account}, &roles)
		if cached {
			v = roles
			return
		}
	}
	roles, rolesErr := fn.store.Bounds(ctx, account)
	if rolesErr != nil {
		err = errors.Warning("rbac: bounds failed").WithCause(rolesErr)
		return
	}
	if fn.cacheable {
		_ = caches.Set(ctx, CacheParam{Account: account}, roles, fn.cacheTTL)
	}
	v = roles
	return
}
