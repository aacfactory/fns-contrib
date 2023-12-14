package rbac

import (
	"fmt"
	"github.com/aacfactory/errors"
	"github.com/aacfactory/fns/services"
)

var (
	endpointName = []byte("rbac")
)

func New(store Store) services.Service {
	if store == nil {
		panic(fmt.Sprintf("%+v", errors.Warning("rbac: store is required")))
		return nil
	}
	return &service{
		Abstract: services.NewAbstract(string(endpointName), true, store),
		store:    store,
	}
}

type service struct {
	services.Abstract
	store Store
}

func (svc *service) Construct(options services.Options) (err error) {
	err = svc.Abstract.Construct(options)
	if err != nil {
		return
	}

	svc.AddFunction(&bindFn{
		store: svc.store,
	})
	svc.AddFunction(&unbindFn{
		store: svc.store,
	})
	svc.AddFunction(&boundsFn{
		store: svc.store,
	})
	svc.AddFunction(&getFn{
		store: svc.store,
	})
	svc.AddFunction(&listFn{
		store: svc.store,
	})
	svc.AddFunction(&saveFn{
		store: svc.store,
	})
	svc.AddFunction(&removeFn{
		store: svc.store,
	})
	return
}
