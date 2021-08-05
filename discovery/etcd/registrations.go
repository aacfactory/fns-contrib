package etcd

import (
	"fmt"
	"github.com/aacfactory/discovery"
	"github.com/dgraph-io/ristretto"
	"sync"
	"time"
)

const (
	defaultServiceCacheNumCounters = 128 * (1 << 20) / 100
	defaultServiceCacheMaxCost     = 128 * (1 << 20)
)

func newRegistrationCache() (c *registrationCache, err error) {

	cache, newCacheErr := ristretto.NewCache(&ristretto.Config{
		NumCounters: defaultServiceCacheNumCounters, // number of keys to track frequency of (10M).
		MaxCost:     defaultServiceCacheMaxCost,     // maximum cost of cache (1GB).
		BufferItems: 64,                             // number of keys per Get buffer.
	})
	if newCacheErr != nil {
		err = fmt.Errorf("discovery make registration cache failed, %v", newCacheErr)
		return
	}

	c = &registrationCache{
		lock:  sync.Mutex{},
		cache: cache,
	}

	return
}

type registrationCache struct {
	lock  sync.Mutex
	cache *ristretto.Cache
}

func (c *registrationCache) put(registration discovery.Registration, revision int64) {
	c.lock.Lock()
	defer c.lock.Unlock()
	storedMap, has := c.getRegistrationRevision(registration.Name)
	if !has {
		storedMap = make(map[string]registrationRevision)
	}
	stored, exist := storedMap[registration.Id]
	if exist && stored.Revision < revision {
		stored.Registration = registration
		stored.Revision = revision
	}
	storedMap[registration.Id] = stored
	c.cache.SetWithTTL(registration.Name, storedMap, int64(len(storedMap)), 24*time.Hour)
	c.cache.Wait()
}

func (c *registrationCache) remove(registration discovery.Registration) {
	c.lock.Lock()
	defer c.lock.Unlock()
	storedMap, has := c.getRegistrationRevision(registration.Name)
	if !has {
		return
	}
	delete(storedMap, registration.Id)
	if len(storedMap) == 0 {
		c.cache.Del(registration.Name)
	} else {
		c.cache.SetWithTTL(registration.Name, storedMap, int64(len(storedMap)), 24*time.Hour)
	}
	c.cache.Wait()
}

func (c *registrationCache) get(name string) (registrations map[string]discovery.Registration, has bool) {
	storedMap, ok := c.getRegistrationRevision(name)
	if !ok {
		return
	}
	if storedMap == nil || len(storedMap) == 0 {
		return
	}

	for id, revision := range storedMap {
		registrations[id] = revision.Registration
	}
	has = true
	return
}

func (c *registrationCache) getRegistrationRevision(name string) (registrations map[string]registrationRevision, has bool) {
	v, existed := c.cache.Get(name)
	if !existed {
		return
	}
	storedMap, ok := v.(map[string]registrationRevision)
	if !ok {
		return
	}
	if storedMap == nil || len(storedMap) == 0 {
		return
	}

	registrations = storedMap
	has = true
	return
}

func (c *registrationCache) close() {
	c.lock.Lock()
	defer c.lock.Unlock()
	c.cache.Clear()
	c.cache.Close()
}

type registrationRevision struct {
	Registration discovery.Registration
	Revision     int64
}
