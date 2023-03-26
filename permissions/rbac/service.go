package rbac

import (
	"context"
	"fmt"
	"github.com/aacfactory/errors"
	"github.com/aacfactory/fns/service"
)

const (
	name     = "rbac"
	saveFn   = "save"
	removeFn = "remove"
	getFn    = "getFn"
	listFn   = "listFn"
	bindFn   = "bind"
	boundsFn = "bounds"
)

func Service(store Store) service.Service {
	if store == nil {
		panic(fmt.Sprintf("%+v", errors.Warning("rbac: store is required")))
		return nil
	}
	return &service_{
		Abstract: service.NewAbstract(name, true, store),
	}
}

type service_ struct {
	service.Abstract
	store Store
}

func (svc *service_) Build(options service.Options) (err error) {
	err = svc.Abstract.Build(options)
	if err != nil {
		return
	}
	if svc.Components() == nil || len(svc.Components()) != 1 {
		err = errors.Warning("rbac: build failed").WithCause(errors.Warning("rbac: store is required"))
		return
	}
	for _, component := range svc.Components() {
		store, ok := component.(Store)
		if !ok {
			err = errors.Warning("rbac: build failed").WithCause(errors.Warning("rbac: store is required"))
			return
		}
		svc.store = store
	}
	return
}

func (svc *service_) Handle(ctx context.Context, fn string, argument service.Argument) (v interface{}, err errors.CodeError) {
	switch fn {
	case saveFn:
		param := SaveRoleParam{}
		paramErr := argument.As(&param)
		if paramErr != nil {
			err = errors.Warning("rbac: save role failed").WithCause(paramErr)
			break
		}
		err = svc.store.Save(ctx, param)
		break
	case removeFn:
		param := ""
		paramErr := argument.As(&param)
		if paramErr != nil {
			err = errors.Warning("rbac: remove role failed").WithCause(paramErr)
			break
		}
		err = svc.store.Remove(ctx, param)
		break
	case getFn:
		param := ""
		paramErr := argument.As(&param)
		if paramErr != nil {
			err = errors.Warning("rbac: get role failed").WithCause(paramErr)
			break
		}
		v, err = svc.store.Get(ctx, param)
		break
	case listFn:
		param := make([]string, 0, 1)
		paramErr := argument.As(&param)
		if paramErr != nil {
			err = errors.Warning("rbac: list role failed").WithCause(paramErr)
			break
		}
		v, err = svc.store.List(ctx, param)
		break
	case bindFn:
		param := BindParam{}
		paramErr := argument.As(&param)
		if paramErr != nil {
			err = errors.Warning("rbac: user bind roles failed").WithCause(paramErr)
			break
		}
		err = svc.store.Bind(ctx, param)
		break
	case boundsFn:
		param := ""
		paramErr := argument.As(&param)
		if paramErr != nil {
			err = errors.Warning("rbac: get user roles failed").WithCause(paramErr)
			break
		}
		err = svc.store.Bounds(ctx, param)
		break
	default:
		err = errors.Warning("rbac: fn was not found")
		break
	}
	return
}
