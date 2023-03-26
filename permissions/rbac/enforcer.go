package rbac

import (
	"context"
	"github.com/aacfactory/errors"
	"github.com/aacfactory/fns/service"
	"github.com/aacfactory/fns/service/builtin/permissions"
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

func (e *enforcer) Build(options service.ComponentOptions) (err error) {
	e.log = options.Log
	return
}

func (e *enforcer) Close() {
	return
}

func (e *enforcer) Enforce(ctx context.Context, param permissions.EnforceParam) (ok bool, err errors.CodeError) {
	if param.UserId == "" {
		err = errors.Warning("rbac: enforce failed").WithCause(errors.Warning("user id is required"))
		return
	}
	if param.Service == "" {
		err = errors.Warning("rbac: enforce failed").WithCause(errors.Warning("service is required"))
		return
	}
	if param.Fn == "" {
		err = errors.Warning("rbac: enforce failed").WithCause(errors.Warning("fn is required"))
		return
	}
	roles, rolesErr := Bounds(ctx, param.UserId.String())
	if rolesErr != nil {
		err = errors.Warning("rbac: enforce failed").WithCause(rolesErr)
		return
	}
	if roles == nil || len(roles) == 0 {
		return
	}
	for _, role := range roles {
		ok = role.CheckPolicy(param.Service, param.Fn)
		if ok {
			return
		}
	}
	return
}
