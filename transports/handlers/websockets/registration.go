package websockets

import (
	"github.com/aacfactory/errors"
	"github.com/aacfactory/fns/context"
	"github.com/aacfactory/fns/runtime"
	"github.com/aacfactory/fns/services"
	"github.com/aacfactory/logs"
	"sync"
	"time"
)

const (
	registrationComponentName = "registration"
)

func LoadRegistration(ctx context.Context) (r Registration, has bool) {
	v, exist := services.LoadComponent[Registration](ctx, _endpointName, registrationComponentName)
	if !exist {
		return
	}
	r, has = v.(Registration)
	return
}

type AbstractRegistration struct {
	ids sync.Map
}

func (registration *AbstractRegistration) Name() (name string) {
	return registrationComponentName
}

type Registration interface {
	services.Component
	Get(ctx context.Context, id []byte) (endpointId []byte, has bool, err error)
	Set(ctx context.Context, id []byte, endpointId []byte, ttl time.Duration) (err error)
	Remove(ctx context.Context, id []byte) (err error)
}

type defaultRegistration struct {
	AbstractRegistration
	log    logs.Logger
	prefix []byte
}

func (registration *defaultRegistration) Construct(options services.Options) (err error) {
	registration.log = options.Log
	registration.prefix = []byte("fns:websockets:")
	return
}

func (registration *defaultRegistration) Shutdown(ctx context.Context) {
	store := runtime.SharedStore(ctx)
	registration.ids.Range(func(key, value any) bool {
		id := key.([]byte)
		_ = store.Remove(ctx, append(registration.prefix, id...))
		return true
	})
	return
}

func (registration *defaultRegistration) Get(ctx context.Context, id []byte) (endpointId []byte, has bool, err error) {
	key := append(registration.prefix, id...)
	endpointId, has, err = runtime.SharedStore(ctx).Get(ctx, key)
	if err != nil {
		err = errors.Warning("websockets: registration get failed").WithCause(err)
		return
	}
	return
}

func (registration *defaultRegistration) Set(ctx context.Context, id []byte, endpointId []byte, ttl time.Duration) (err error) {
	key := append(registration.prefix, id...)
	if ttl < 0 {
		ttl = 1 * time.Hour
	}
	err = runtime.SharedStore(ctx).SetWithTTL(ctx, key, endpointId, ttl)
	if err != nil {
		err = errors.Warning("websockets: registration set failed").WithCause(err)
		return
	}
	registration.ids.Store(id, struct{}{})
	return
}

func (registration *defaultRegistration) Remove(ctx context.Context, id []byte) (err error) {
	key := append(registration.prefix, id...)
	err = runtime.SharedStore(ctx).Remove(ctx, key)
	if err != nil {
		err = errors.Warning("websockets: registration remove failed").WithCause(err)
		return
	}
	registration.ids.Delete(id)
	return
}
