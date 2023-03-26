package rbac

import (
	"context"
	"github.com/aacfactory/errors"
	"github.com/aacfactory/fns/service"
	"github.com/aacfactory/fns/service/builtin/permissions"
)

func Enforcer() permissions.Enforcer {
	return &enforcer{}
}

type enforcer struct {
}

func (e *enforcer) Name() (name string) {
	name = "rbac"
	return
}

func (e *enforcer) Build(options service.ComponentOptions) (err error) {

	return
}

func (e *enforcer) Close() {

	return
}

func (e *enforcer) Enforce(ctx context.Context, param permissions.EnforceParam) (ok bool, err errors.CodeError) {

	return
}
