package rbac

import (
	"fmt"
	"github.com/aacfactory/errors"
	"github.com/aacfactory/fns/commons/bytex"
	"github.com/aacfactory/fns/context"
	"github.com/aacfactory/fns/services"
	"github.com/aacfactory/fns/services/authorizations"
	"time"
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
	config := Config{}
	configErr := options.Config.As(&config)
	if configErr != nil {
		err = errors.Warning("rbac: construct failed").WithCause(configErr)
		return
	}
	cacheable := config.Cache.Enable
	cacheTTL := config.Cache.TTL
	if cacheable && cacheTTL < 1 {
		cacheTTL = 1 * time.Hour
	}
	svc.AddFunction(&bindFn{
		store:     svc.store,
		cacheable: cacheable,
	})
	svc.AddFunction(&unbindFn{
		store:     svc.store,
		cacheable: cacheable,
	})
	svc.AddFunction(&boundsFn{
		store:     svc.store,
		cacheable: cacheable,
		cacheTTL:  cacheTTL,
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

const (
	cachePrefix = "rbac:"
)

type CacheParam struct {
	Account authorizations.Id
}

func (param CacheParam) CacheKey(ctx context.Context) (key []byte, err error) {
	key = append(bytex.FromString(cachePrefix), param.Account...)
	return
}
