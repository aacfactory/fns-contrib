package rbac

import (
	"github.com/aacfactory/errors"
	"github.com/aacfactory/fns/context"
	"github.com/aacfactory/fns/services"
	"github.com/aacfactory/fns/services/permissions"
	"github.com/aacfactory/logs"
)

func Enforcer() permissions.Enforcer {
	return &enforcer{}
}

type enforcer struct {
	log logs.Logger
}

func (e *enforcer) Name() (name string) {
	name = "rbac"
	return
}

func (e *enforcer) Construct(options services.Options) (err error) {
	e.log = options.Log
	return
}

func (e *enforcer) Shutdown(_ context.Context) {
	return
}

func (e *enforcer) Enforce(ctx context.Context, param permissions.EnforceParam) (ok bool, err error) {
	if !param.Account.Exist() {
		err = errors.Warning("rbac: enforce failed").WithCause(errors.Warning("account is required"))
		return
	}
	if param.Endpoint == "" {
		err = errors.Warning("rbac: enforce failed").WithCause(errors.Warning("endpoint is required"))
		return
	}
	if param.Fn == "" {
		err = errors.Warning("rbac: enforce failed").WithCause(errors.Warning("fn is required"))
		return
	}
	roles, rolesErr := Bounds(ctx, param.Account)
	if rolesErr != nil {
		err = errors.Warning("rbac: enforce failed").WithCause(rolesErr)
		return
	}
	ok = roles.CheckPolicy(param.Endpoint, param.Fn)
	return
}
