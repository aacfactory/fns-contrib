package sql

import (
	"fmt"
	"github.com/aacfactory/fns"
	"github.com/aacfactory/json"
	"reflect"
	"sync"
)

type DAOConfig struct {
	CacheKind string          `json:"cacheKind,omitempty"`
	Raw       json.RawMessage `json:"raw,omitempty"`
}

type DaoCache interface {
	GetAndFill(row TableRow) (has bool, synced bool)
	Set(row TableRow, synced bool)
	Clean()
}

// +-------------------------------------------------------------------------------------------------------------------+

var (
	daoCache     DaoCache = nil
	daoCacheOnce          = sync.Once{}
)

func getDAOCache(ctx fns.Context) (cache DaoCache) {
	daoCacheOnce.Do(func() {
		proxy, proxyErr := ctx.App().ServiceProxy(ctx, Namespace)
		if proxyErr != nil {
			if ctx.App().Log().ErrorEnabled() {
				ctx.App().Log().Error().Message("fns SQL: use dao but can not get dao cache from sql service. use local dao cache insteadof")
			}
			daoCache = newLocalDaoCache()
			return
		}

		arg, _ := fns.NewArgument(&fns.Empty{})
		r := proxy.Request(ctx, daoCacheConfigLoadFn, arg)
		config := DAOConfig{}
		getErr := r.Get(ctx, &config)
		if getErr != nil {
			if ctx.App().Log().ErrorEnabled() {
				ctx.App().Log().Error().Message("fns SQL: use dao but can not get dao cache from sql service. use local dao cache insteadof")
			}
			daoCache = newLocalDaoCache()
			return
		}
		switch config.CacheKind {
		case "local":
			daoCache = newLocalDaoCache()
		case "redis":
			// todo
		default:
			daoCache = newLocalDaoCache()
		}

	})
	cache = daoCache
	return
}

// +-------------------------------------------------------------------------------------------------------------------+

type cachedTableRow struct {
	value  TableRow
	synced bool
}

func newLocalDaoCache() (cache DaoCache) {
	cache = &localDaoCache{
		values: sync.Map{},
	}
	return
}

type localDaoCache struct {
	values sync.Map
}

func (cache *localDaoCache) buildKey(row TableRow) (key string) {
	info := getTableRowInfo(row)
	rv := reflect.Indirect(reflect.ValueOf(row))
	pks := make([]interface{}, 0, 1)
	for _, pk := range info.Pks {
		pks = append(pks, rv.FieldByName(pk.StructFieldName).Interface())
	}
	for i, value := range pks {
		if i == 0 {
			key = fmt.Sprintf("%v", value)
		} else {
			key = key + "," + fmt.Sprintf("%v", value)
		}
	}
	return
}

func (cache *localDaoCache) GetAndFill(row TableRow) (has bool, synced bool) {
	v, loaded := cache.values.Load(cache.buildKey(row))
	if loaded {
		has = true
		cached := v.(*cachedTableRow)
		reflect.ValueOf(row).Elem().Set(reflect.ValueOf(cached.value).Elem())
		synced = cached.synced
	}
	return
}

func (cache *localDaoCache) Set(row TableRow, synced bool) {
	var cached *cachedTableRow
	v, loaded := cache.values.Load(cache.buildKey(row))
	if loaded {
		cached = v.(*cachedTableRow)
		cached.value = row
		cached.synced = synced
	} else {
		cached = &cachedTableRow{
			value:  row,
			synced: synced,
		}
	}
	cache.values.Store(cache.buildKey(row), cached)
}

func (cache *localDaoCache) Clean() {
	keys := make([]interface{}, 0, 1)
	cache.values.Range(func(key, value interface{}) bool {
		keys = append(keys, key)
		return true
	})
	for _, key := range keys {
		cache.values.Delete(key)
	}
}
