package shareds

import (
	"context"
	"fmt"
	"github.com/aacfactory/fns-contrib/databases/redis"
	"github.com/aacfactory/fns/commons/bytex"
	"github.com/aacfactory/fns/service/shareds"
	"time"
)

func Cache(defaultTTL time.Duration) shareds.Caches {
	if defaultTTL < 1 {
		defaultTTL = 30 * time.Minute
	}
	return &cache{
		defaultTTL: defaultTTL,
	}
}

type cache struct {
	defaultTTL time.Duration
}

func (c *cache) Get(ctx context.Context, key []byte) (value []byte, has bool) {
	v, getErr := redis.Get(ctx, bytex.ToString(c.makeKey(key)))
	if getErr != nil {
		return
	}
	has = v.Has
	if has {
		value = bytex.FromString(v.Value)
	}
	return
}

func (c *cache) Exist(ctx context.Context, key []byte) (has bool) {
	v, existErr := redis.Exists(ctx, bytex.ToString(c.makeKey(key)))
	if existErr != nil {
		return
	}
	has = v
	return
}

func (c *cache) Set(ctx context.Context, key []byte, value []byte, ttl time.Duration) (ok bool) {
	if ttl < 1 {
		ttl = c.defaultTTL
	}
	setErr := redis.Set(ctx, redis.SetParam{
		Key:        bytex.ToString(c.makeKey(key)),
		Value:      bytex.ToString(value),
		Expiration: ttl,
	})
	ok = setErr == nil
	return
}

func (c *cache) Remove(ctx context.Context, key []byte) {
	_ = redis.Del(ctx, []string{bytex.ToString(c.makeKey(key))})
	return
}

func (c *cache) makeKey(key []byte) []byte {
	return bytex.FromString(fmt.Sprintf("fns/shareds/caches/%s", bytex.ToString(key)))
}
