package tokens

import (
	"context"
	"fmt"
	"github.com/aacfactory/errors"
	"github.com/aacfactory/fns/service"
)

const (
	name     = "tokens"
	saveFn   = "save"
	removeFn = "remove"
	getFn    = "get"
	listFn   = "list"
)

func Service(store Store) service.Service {
	if store == nil {
		panic(fmt.Sprintf("%+v", errors.Warning("tokens: store is required")))
		return nil
	}
	return &service_{
		Abstract: service.NewAbstract(name, true, convertStoreToComponent(store)),
	}
}

type service_ struct {
	service.Abstract
	store Store
}

func (svc service_) Build(options service.Options) (err error) {
	err = svc.Abstract.Build(options)
	if err != nil {
		return
	}
	if svc.Components() == nil {
		err = errors.Warning("tokens: build failed").WithCause(errors.Warning("tokens: components is required"))
		return
	}
	component, has := svc.Components()[storeComponentName]
	if !has {
		err = errors.Warning("tokens: build failed").WithCause(errors.Warning("tokens: store components is required"))
		return
	}
	store, ok := component.(*storeComponent)
	if !ok {
		err = errors.Warning("tokens: build failed").WithCause(errors.Warning("tokens: store is required"))
		return
	}
	svc.store = store.store
	return
}

func (svc service_) Handle(ctx context.Context, fn string, argument service.Argument) (v interface{}, err errors.CodeError) {
	switch fn {
	case saveFn:
		param := SaveParam{}
		paramErr := argument.As(&param)
		if paramErr != nil {
			err = errors.Warning("tokens: save token failed").WithCause(paramErr)
			break
		}
		err = svc.store.Save(ctx, param)
		break
	case removeFn:
		param := RemoveParam{}
		paramErr := argument.As(&param)
		if paramErr != nil {
			err = errors.Warning("tokens: remove token failed").WithCause(paramErr)
			break
		}
		err = svc.store.Remove(ctx, param)
		break
	case getFn:
		param := ""
		paramErr := argument.As(&param)
		if paramErr != nil {
			err = errors.Warning("tokens: get token failed").WithCause(paramErr)
			break
		}
		has := false
		v, has, err = svc.store.Get(ctx, param)
		if err == nil && !has {
			err = ErrTokenNotFound
		}
		break
	case listFn:
		param := ""
		paramErr := argument.As(&param)
		if paramErr != nil {
			err = errors.Warning("tokens: list tokens failed").WithCause(paramErr)
			break
		}
		v, err = svc.store.List(ctx, param)
		break
	default:
		err = errors.Warning("tokens: fn was not found")
		break
	}
	return
}
