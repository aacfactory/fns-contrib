package rbac

import (
	"github.com/aacfactory/errors"
	"github.com/aacfactory/fns/context"
	"github.com/aacfactory/fns/runtime"
	"github.com/aacfactory/fns/services"
)

var (
	listFnName = []byte("list")
)

func List(ctx context.Context) (v Roles, err error) {
	r, rErr := runtime.Endpoints(ctx).Request(ctx, endpointName, listFnName, nil)
	if rErr != nil {
		err = rErr
		return
	}
	v, err = services.ValueOfResponse[Roles](r)
	if err != nil {
		err = errors.Warning("rbac: list failed").WithCause(err)
		return
	}
	return
}

type listFn struct {
	store Store
}

func (fn *listFn) Name() string {
	return string(listFnName)
}

func (fn *listFn) Internal() bool {
	return true
}

func (fn *listFn) Readonly() bool {
	return false
}

func (fn *listFn) Handle(ctx services.Request) (v any, err error) {
	roles, rolesErr := fn.store.Roles(ctx)
	if rolesErr != nil {
		err = errors.Warning("rbac: list failed").WithCause(rolesErr)
		return
	}
	v = roles
	return
}
