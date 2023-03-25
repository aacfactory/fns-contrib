package tokens

import (
	"context"
	"fmt"
	"github.com/aacfactory/errors"
	"github.com/aacfactory/fns/service"
)

const (
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
		Abstract: service.NewAbstract("tokens", true, store),
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
	if svc.Components() == nil || len(svc.Components()) != 1 {
		err = errors.Warning("tokens: build failed").WithCause(errors.Warning("tokens: store is required"))
		return
	}
	for _, component := range svc.Components() {
		store, ok := component.(Store)
		if !ok {
			err = errors.Warning("tokens: build failed").WithCause(errors.Warning("tokens: store is required"))
			return
		}
		svc.store = store
	}
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
		v, err = svc.store.Get(ctx, param)
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
