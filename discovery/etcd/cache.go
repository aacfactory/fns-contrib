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
		lock:        sync.Mutex{},
		groupKeyMap: make(map[string]string),
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
	lock        sync.Mutex
	cache       *ristretto.Cache
	groupKeyMap map[string]string
}

func (c *proxyCache) onEvict(item *ristretto.Item) {
	group, ok := item.Value.(*fns.RemotedServiceProxyGroup)
	if !ok {
		return
	}
	c.lock.Lock()
	defer c.lock.Unlock()
	delete(c.groupKeyMap, group.Namespace())
	group.Close()
}

func (c *proxyCache) put(registration Registration) {
	c.lock.Lock()
	defer c.lock.Unlock()
	group, has := c.get(registration.Name)
	if !has {
		group = fns.NewRemotedServiceProxyGroup(registration.Name)
	}
	if group.ContainsAgent(registration.Id) {
		return
	}
	agent := fns.NewRemotedServiceProxy(registration.Id, registration.Name, registration.Address)
	if !agent.Health() {
		return
	}
	group.AppendAgent(agent)

	c.cache.SetWithTTL(registration.Name, group, int64(64), 24*time.Hour)
	c.cache.Wait()

	c.groupKeyMap[group.Namespace()] = group.Namespace()

}

func (c *proxyCache) get(namespace string) (group *fns.RemotedServiceProxyGroup, has bool) {
	v, existed := c.cache.Get(namespace)
	if !existed {
		return
	}
	group, has = v.(*fns.RemotedServiceProxyGroup)
	if !has || group == nil {
		c.cache.Del(namespace)
		c.cache.Wait()
		has = false
		return
	}

	return
}

func (c *proxyCache) getProxy(id string) (proxy *fns.RemotedServiceProxy, has bool) {
	for key := range c.groupKeyMap {
		group, hasGroup := c.get(key)
		if hasGroup {
			agent, agentErr := group.GetAgent(id)
			if agentErr == nil {
				proxy = agent
				has = true
				return
			}
		}
	}
	return
}

func (c *proxyCache) remove(registration Registration) {
	c.lock.Lock()
	defer c.lock.Unlock()
	group, has := c.get(registration.Name)
	if !has {
		return
	}
	group.RemoveAgent(registration.Id)
	if group.AgentNum() == 0 {
		c.cache.Del(registration.Name)
		delete(c.groupKeyMap, group.Namespace())
	} else {
		c.cache.SetWithTTL(registration.Name, group, int64(64), 24*time.Hour)
	}
	c.cache.Wait()

}

func (c *proxyCache) close() {
	c.lock.Lock()
	defer c.lock.Unlock()
	c.cache.Clear()
	c.cache.Close()
}
