package etcd

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
		lock: sync.Mutex{},
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
	lock  sync.Mutex
	cache *ristretto.Cache
}

func (c *proxyCache) onEvict(item *ristretto.Item) {
	proxy, ok := item.Value.(*fns.GroupRemotedServiceProxy)
	if !ok {
		return
	}
	proxy.Close()
}

func (c *proxyCache) put(registration Registration) {
	c.lock.Lock()
	defer c.lock.Unlock()
	proxy, has := c.get(registration.Name)
	if !has {
		proxy = fns.NewGroupRemotedServiceProxy(registration.Name)
	}
	if proxy.ContainsAgent(registration.Id) {
		return
	}
	agent := fns.NewRemotedServiceProxy(registration.Id, registration.Name, registration.Address)
	if !agent.Health() {
		return
	}
	proxy.AppendAgent(agent)

	c.cache.SetWithTTL(registration.Name, proxy, int64(64), 24*time.Hour)
	c.cache.Wait()

}

func (c *proxyCache) get(namespace string) (proxy *fns.GroupRemotedServiceProxy, has bool) {
	v, existed := c.cache.Get(namespace)
	if !existed {
		return
	}
	proxy, has = v.(*fns.GroupRemotedServiceProxy)
	if !has || proxy == nil {
		c.cache.Del(namespace)
		c.cache.Wait()
		has = false
		return
	}

	return
}

func (c *proxyCache) remove(registration Registration) {
	c.lock.Lock()
	defer c.lock.Unlock()
	proxy, has := c.get(registration.Name)
	if !has {
		return
	}
	proxy.RemoveAgent(registration.Id)
	if proxy.AgentNum() == 0 {
		c.cache.Del(registration.Name)
	} else {
		c.cache.SetWithTTL(registration.Name, proxy, int64(64), 24*time.Hour)
	}
	c.cache.Wait()
}

func (c *proxyCache) close() {
	c.lock.Lock()
	defer c.lock.Unlock()
	c.cache.Clear()
	c.cache.Close()
}
