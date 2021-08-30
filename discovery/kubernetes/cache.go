package kubernetes

import (
	"fmt"
	"github.com/aacfactory/fns"
	"github.com/dgraph-io/ristretto"
	"sync"
	"time"
)

const (
	defaultRemoteCacheNumCounters = 128 * (1 << 20) / 100
	defaultRemoteCacheMaxCost     = 128 * (1 << 20)
)

func newProxyCache() (c *proxyCache, err error) {

	c = &proxyCache{
		lock:   sync.Mutex{},
		active: make(map[string]string),
	}

	cache, newCacheErr := ristretto.NewCache(&ristretto.Config{
		NumCounters: defaultRemoteCacheNumCounters, // number of keys to track frequency of (10M).
		MaxCost:     defaultRemoteCacheMaxCost,     // maximum cost of cache (1GB).
		BufferItems: 64,                            // number of keys per Get buffer.
		OnEvict:     c.onEvict,
	})

	if newCacheErr != nil {
		err = fmt.Errorf("discovery make registration cache failed, %v", newCacheErr)
		return
	}

	c.cache = cache

	return
}

type proxyCache struct {
	lock   sync.Mutex
	active map[string]string
	cache  *ristretto.Cache
}

func (c *proxyCache) onEvict(item *ristretto.Item) {
	c.lock.Lock()
	defer c.lock.Unlock()
	proxy, ok := item.Value.(*fns.RemotedServiceProxy)
	if !ok {
		return
	}
	proxy.Close()
	delete(c.active, proxy.Namespace())
}

func (c *proxyCache) check() {
	inactive := make([]string, 0, 1)
	for key := range c.active {
		proxy, has := c.get(key)
		if !has {
			continue
		}
		if !proxy.Check() {
			inactive = append(inactive, key)
		}
	}
	for _, key := range inactive {
		c.remove(key)
	}
}

func (c *proxyCache) put(registration Registration) (proxy *fns.RemotedServiceProxy) {
	c.lock.Lock()
	defer c.lock.Unlock()
	has := false
	proxy, has = c.get(registration.Name)
	if has {
		return
	}

	proxy = fns.NewRemotedServiceProxy(fns.UID(), registration.Name, registration.Address)
	if !proxy.Health() {
		return
	}

	c.cache.SetWithTTL(registration.Name, proxy, int64(64), 24*time.Hour)
	c.cache.Wait()

	c.active[registration.Name] = registration.Address

	return
}

func (c *proxyCache) get(namespace string) (proxy *fns.RemotedServiceProxy, has bool) {
	v, existed := c.cache.Get(namespace)
	if !existed {
		return
	}
	proxy, has = v.(*fns.RemotedServiceProxy)
	if !has || proxy == nil {
		c.cache.Del(namespace)
		c.cache.Wait()
		has = false
		return
	}

	return
}

func (c *proxyCache) remove(key string) {
	c.lock.Lock()
	defer c.lock.Unlock()
	proxy, has := c.get(key)
	if !has {
		return
	}

	proxy.Close()

	c.cache.Del(key)

	c.cache.Wait()

	delete(c.active, key)
}

func (c *proxyCache) close() {
	c.lock.Lock()
	defer c.lock.Unlock()
	c.cache.Clear()
	c.cache.Close()
	c.active = make(map[string]string)
}
