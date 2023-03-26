package rbac

import (
	"context"
	"github.com/aacfactory/errors"
	"github.com/aacfactory/fns/service"
	"github.com/aacfactory/fns/service/builtin/permissions"
)

type Enforcer struct {
}

func (enforcer *Enforcer) Name() (name string) {
	name = "rbac"
	return
}

func (enforcer *Enforcer) Build(options service.ComponentOptions) (err error) {

	return
}

func (enforcer *Enforcer) Close() {

	return
}

func (enforcer *Enforcer) Enforce(ctx context.Context, param permissions.EnforceParam) (ok bool, err errors.CodeError) {

	return
}
