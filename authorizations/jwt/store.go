package jwt

import (
	"context"
	"fmt"
	"github.com/aacfactory/errors"
	"github.com/aacfactory/fns"
	"github.com/aacfactory/json"
	"github.com/dgraph-io/ristretto"
	"strings"
	"time"
)

func NewStore(config StoreConfig) (s Store, err error) {

	if config.Kind == "" || config.Kind == "memory" {
		s, err = newMemoryStore()
	} else if config.Kind == "service" {
		namespace := strings.TrimSpace(config.Namespace)
		if namespace == "" {
			err = fmt.Errorf("fns JWT Store New: namespace is empty")
			return
		}
		activeTokenFn := strings.TrimSpace(config.ActiveTokenFn)
		if activeTokenFn == "" {
			err = fmt.Errorf("fns JWT Store New: activeTokenFn is empty")
			return
		}
		lookUpTokenFn := strings.TrimSpace(config.LookUpTokenFn)
		if lookUpTokenFn == "" {
			err = fmt.Errorf("fns JWT Store New: lookUpTokenFn is empty")
			return
		}
		revokeTokenFn := strings.TrimSpace(config.RevokeTokenFn)
		if revokeTokenFn == "" {
			err = fmt.Errorf("fns JWT Store New: revokeTokenFn is empty")
			return
		}
		s = &serviceStore{
			namespace:     namespace,
			activeTokenFn: activeTokenFn,
			lookUpTokenFn: lookUpTokenFn,
			revokeTokenFn: revokeTokenFn,
		}
	} else {
		err = fmt.Errorf("fns JWT Store New: kind is not supported")
		return
	}

	return
}

type Store interface {
	Active(ctx fns.Context, id string, expiration time.Duration) (err error)
	Revoke(ctx fns.Context, id string) (err error)
	LookUp(ctx fns.Context, id string) (has bool)
}

const (
	defaultCacheNumCounters = 128 * (1 << 20) / 100
	defaultCacheMaxCost     = 128 * (1 << 20)
)

func newMemoryStore() (s *memoryStore, err error) {

	cache, newCacheErr := ristretto.NewCache(&ristretto.Config{
		NumCounters: defaultCacheNumCounters, // number of keys to track frequency of (10M).
		MaxCost:     defaultCacheMaxCost,     // maximum cost of cache (1GB).
		BufferItems: 64,                      // number of keys per Get buffer.
	})

	if newCacheErr != nil {
		err = fmt.Errorf("memory store create cache failed, %v", newCacheErr)
		return
	}

	s = &memoryStore{
		cache: cache,
	}

	return
}

type memoryStore struct {
	cache *ristretto.Cache
}

func (s *memoryStore) Active(_ fns.Context, id string, expiration time.Duration) (err error) {
	s.cache.SetWithTTL(id, id, int64(len(id)), expiration)
	s.cache.Wait()
	return
}

func (s *memoryStore) Revoke(_ fns.Context, id string) (err error) {
	s.cache.Del(id)
	s.cache.Wait()
	return
}

func (s *memoryStore) LookUp(_ fns.Context, id string) (has bool) {
	_, has = s.cache.Get(id)
	return
}

// +-------------------------------------------------------------------------------------------------------------------+

type ActiveArg struct {
	Id         string        `json:"id,omitempty"`
	Expiration time.Duration `json:"expiration,omitempty"`
}

type IdArg struct {
	Id string `json:"id,omitempty"`
}

type LookUpResult struct {
	Has bool `json:"has,omitempty"`
}

type serviceStore struct {
	namespace     string
	activeTokenFn string
	lookUpTokenFn string
	revokeTokenFn string
}

func (s *serviceStore) Active(ctx fns.Context, id string, expiration time.Duration) (err error) {
	proxy, proxyErr := ctx.ServiceProxy(s.namespace)
	if proxyErr != nil {
		err = errors.ServiceError(fmt.Sprintf("fns JWT Store Active: get %s service proxy failed", s.namespace)).WithCause(proxyErr)
		return
	}

	arg, argErr := fns.NewArgument(&ActiveArg{
		Id:         id,
		Expiration: expiration,
	})
	if argErr != nil {
		err = errors.ServiceError(fmt.Sprintf("fns JWT Store Active: make %s service proxy arg failed", s.namespace)).WithCause(argErr)
		return
	}
	result := proxy.Request(ctx, s.activeTokenFn, arg)

	fnErr := result.Get(context.TODO(), &json.RawMessage{})
	if fnErr != nil {
		err = errors.ServiceError(fmt.Sprintf("fns JWT Store Active: invoke %s service %s fn failed", s.namespace, s.activeTokenFn)).WithCause(fnErr)
		return
	}

	return
}

func (s *serviceStore) Revoke(ctx fns.Context, id string) (err error) {
	proxy, proxyErr := ctx.ServiceProxy(s.namespace)
	if proxyErr != nil {
		err = errors.ServiceError(fmt.Sprintf("fns JWT Store Revoke: get %s service proxy failed", s.namespace)).WithCause(proxyErr)
		return
	}

	arg, argErr := fns.NewArgument(&IdArg{
		Id: id,
	})
	if argErr != nil {
		err = errors.ServiceError(fmt.Sprintf("fns JWT Store Revoke: make %s service proxy arg failed", s.namespace)).WithCause(argErr)
		return
	}
	result := proxy.Request(ctx, s.revokeTokenFn, arg)

	fnErr := result.Get(context.TODO(), &json.RawMessage{})
	if fnErr != nil {
		err = errors.ServiceError(fmt.Sprintf("fns JWT Store Revoke: invoke %s service %s fn failed", s.namespace, s.revokeTokenFn)).WithCause(fnErr)
		return
	}

	return
}

func (s *serviceStore) LookUp(ctx fns.Context, id string) (has bool) {
	proxy, proxyErr := ctx.ServiceProxy(s.namespace)
	if proxyErr != nil {
		return
	}

	arg, argErr := fns.NewArgument(&IdArg{
		Id: id,
	})
	if argErr != nil {
		return
	}
	result := proxy.Request(ctx, s.lookUpTokenFn, arg)

	r := LookUpResult{}

	fnErr := result.Get(context.TODO(), &r)
	if fnErr != nil {
		return
	}

	has = r.Has

	return
}
