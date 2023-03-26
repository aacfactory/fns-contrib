package rbac

import (
	"context"
	"fmt"
	"github.com/aacfactory/errors"
	"github.com/aacfactory/fns/service"
)

const (
	name = "rbac"
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
	//TODO implement me
	panic("implement me")
}

func (svc *service_) Handle(ctx context.Context, fn string, argument service.Argument) (v interface{}, err errors.CodeError) {
	//TODO implement me
	panic("implement me")
}
