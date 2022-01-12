package sql

import (
	"crypto/md5"
	"fmt"
	"github.com/aacfactory/fns"
	"github.com/aacfactory/json"
	"github.com/tidwall/gjson"
	"reflect"
	"sync"
	"time"
)

type DAOConfig struct {
	CacheKind string          `json:"cacheKind,omitempty"`
	Raw       json.RawMessage `json:"options,omitempty"`
}

type DaoCache interface {
	GetAndFill(row TableRow) (has bool, synced bool)
	Set(row TableRow, synced bool)
	Remove(row TableRow)
	Clean()
}

// +-------------------------------------------------------------------------------------------------------------------+

var (
	daoCache     DaoCache = nil
	daoCacheOnce          = sync.Once{}
)

func getDAOCache(ctx fns.Context) (cache DaoCache) {
	daoCacheOnce.Do(func() {
		proxy, proxyErr := ctx.App().ServiceProxy(ctx, namespace)
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
			daoCache = newRedisDaoCache(ctx, config.Raw)
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
		if !cached.synced {
			cached.value = row
			cached.synced = synced
		}
	} else {
		cached = &cachedTableRow{
			value:  row,
			synced: synced,
		}
	}
	cache.values.Store(cache.buildKey(row), cached)
}

func (cache *localDaoCache) Remove(row TableRow) {
	cache.values.Delete(cache.buildKey(row))
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

// +-------------------------------------------------------------------------------------------------------------------+

func newRedisDaoCache(ctx fns.Context, configRaw json.RawMessage) (cache DaoCache) {
	ttl := 24 * time.Hour
	ttlNode := gjson.GetBytes(configRaw, "ttl")
	if ttlNode.Exists() {
		ttl0, parseErr := time.ParseDuration(ttlNode.String())
		if parseErr != nil {
			panic(fmt.Sprintf("fns SQL: use DAO failed for parse sql.dao.ttl in config failed"))
		}
		ttl = ttl0
	}
	cache = &redisDaoCache{
		ctx:   ctx,
		local: newLocalDaoCache(),
		ttl:   ttl,
	}
	return
}

type redisDaoCache struct {
	ctx   fns.Context
	local DaoCache
	ttl   time.Duration
}

func (cache *redisDaoCache) GetAndFill(row TableRow) (has bool, synced bool) {
	has, synced = cache.local.GetAndFill(row)
	if has {
		return
	}
	has = cache.getFromRedis(row)
	if has {
		cache.local.Set(row, false)
	}
	return
}

func (cache *redisDaoCache) Set(row TableRow, synced bool) {
	cache.local.Set(row, synced)
	if synced {
		return
	}
	cache.setIntoRedis(row)
	return
}

func (cache *redisDaoCache) Remove(row TableRow) {
	cache.local.Remove(row)
	cache.removeFromRedis(row)
}

type redisGetResult struct {
	Value json.RawMessage `json:"value,omitempty"`
	Has   bool            `json:"has,omitempty"`
}

func (cache *redisDaoCache) getFromRedis(row TableRow) (has bool) {
	key := cache.buildRowCacheKey(row)
	proxy, proxyErr := cache.ctx.App().ServiceProxy(cache.ctx, "redis")
	if proxyErr != nil {
		return
	}
	arg, argErr := fns.NewArgument(key)
	if argErr != nil {
		return
	}
	result := &redisGetResult{}
	r := proxy.Request(cache.ctx, "get", arg)
	fnErr := r.Get(cache.ctx, result)
	if fnErr != nil {
		return
	}
	if !result.Has {
		return
	}
	cache.mapJsonToRow(result.Value, row)
	has = true
	return
}

type redisSetParam struct {
	Key        string          `json:"key,omitempty"`
	Value      json.RawMessage `json:"value,omitempty"`
	Expiration time.Duration   `json:"expiration,omitempty"`
}

func (cache *redisDaoCache) setIntoRedis(row TableRow) {
	key := cache.buildRowCacheKey(row)
	proxy, proxyErr := cache.ctx.App().ServiceProxy(cache.ctx, "redis")
	if proxyErr != nil {
		return
	}
	param := &redisSetParam{
		Key:        key,
		Value:      cache.mapRowToJson(row),
		Expiration: cache.ttl,
	}
	arg, argErr := fns.NewArgument(param)
	if argErr != nil {
		return
	}
	r := proxy.Request(cache.ctx, "set", arg)
	fnErr := r.Get(cache.ctx, &json.RawMessage{})
	if fnErr != nil {
		return
	}
	return
}

func (cache *redisDaoCache) removeFromRedis(row TableRow) {
	key := cache.buildRowCacheKey(row)
	proxy, proxyErr := cache.ctx.App().ServiceProxy(cache.ctx, "redis")
	if proxyErr != nil {
		return
	}
	arg, argErr := fns.NewArgument(key)
	if argErr != nil {
		return
	}
	r := proxy.Request(cache.ctx, "remove", arg)
	fnErr := r.Get(cache.ctx, &json.RawMessage{})
	if fnErr != nil {
		return
	}
	return
}

func (cache *redisDaoCache) Clean() {
	cache.local.Clean()
	return
}

func (cache *redisDaoCache) mapRowToJson(row TableRow) (p []byte) {
	o := json.NewObject()
	info := getTableRowInfo(row)
	rv := reflect.Indirect(reflect.ValueOf(row))
	if info.Pks != nil {
		for _, pk := range info.Pks {
			pkv := rv.FieldByName(pk.StructFieldName).Interface()
			_ = o.Put(pk.StructFieldName, pkv)
		}
	}
	if info.CreateBY != nil {
		x := rv.FieldByName(info.CreateBY.StructFieldName).Interface()
		_ = o.Put(info.CreateBY.StructFieldName, x)
	}
	if info.CreateAT != nil {
		x := rv.FieldByName(info.CreateAT.StructFieldName).Interface()
		_ = o.Put(info.CreateAT.StructFieldName, x)
	}
	if info.ModifyBY != nil {
		x := rv.FieldByName(info.ModifyBY.StructFieldName).Interface()
		_ = o.Put(info.ModifyBY.StructFieldName, x)
	}
	if info.ModifyAT != nil {
		x := rv.FieldByName(info.ModifyAT.StructFieldName).Interface()
		_ = o.Put(info.ModifyAT.StructFieldName, x)
	}
	if info.DeleteBY != nil {
		x := rv.FieldByName(info.DeleteBY.StructFieldName).Interface()
		_ = o.Put(info.DeleteBY.StructFieldName, x)
	}
	if info.DeleteAT != nil {
		x := rv.FieldByName(info.DeleteAT.StructFieldName).Interface()
		_ = o.Put(info.DeleteAT.StructFieldName, x)
	}
	if info.Version != nil {
		x := rv.FieldByName(info.Version.StructFieldName).Interface()
		_ = o.Put(info.Version.StructFieldName, x)
	}
	if info.Columns != nil {
		for _, col := range info.Columns {
			x := rv.FieldByName(col.StructFieldName).Interface()
			_ = o.Put(col.StructFieldName, x)
		}
	}
	if info.ForeignColumns != nil {
		for _, col := range info.ForeignColumns {
			fcv := rv.FieldByName(col.StructFieldName)
			if fcv.IsNil() {
				continue
			}
			fc := fcv.Interface()
			fcInfo := getTableRowInfo(fc)
			fcPkv := fcv.Elem().FieldByName(fcInfo.Pks[0].StructFieldName).Interface()
			_ = o.Put(col.StructFieldName, map[string]interface{}{fcInfo.Pks[0].StructFieldName: fcPkv})
		}
	}
	p = o.Raw()
	return
}

func (cache *redisDaoCache) mapJsonToRow(p []byte, row TableRow) {
	err := json.Unmarshal(p, row)
	if err != nil {
		panic(fmt.Sprintf("fns SQL: use DAO failed for decode redis cache failed, %v", err))
	}
	return
}

func (cache *redisDaoCache) buildRowCacheKey(row TableRow) (key string) {
	info := getTableRowInfo(row)
	if info.Pks == nil || len(info.Pks) == 0 {
		p := cache.mapRowToJson(row)
		key = fmt.Sprintf("fns_dao:%s:%s:%x", info.Schema, info.Name, md5.Sum(p))
		return
	}
	rv := reflect.Indirect(reflect.ValueOf(row))
	for _, pk := range info.Pks {
		key = key + "-" + fmt.Sprintf("%v", rv.FieldByName(pk.StructFieldName).Interface())
	}
	key = "fns_dao:" + info.Schema + ":" + info.Name + ":" + key[1:]
	return
}
