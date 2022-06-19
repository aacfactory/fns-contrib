package internal

import (
	"context"
	"github.com/aacfactory/errors"
	"github.com/go-redis/redis/v8"
)

const (
	SADD        = "SADD"
	SCARD       = "SCARD"
	SDIFF       = "SDIFF"
	SDIFFSTORE  = "SDIFFSTORE"
	SINTER      = "SINTER"
	SINTERSTORE = "SINTERSTORE"
	SISMEMBER   = "SISMEMBER"
	SMEMBERS    = "SMEMBERS"
	SMOVE       = "SMOVE"
	SPOP        = "SPOP"
	SRANDMEMBER = "SRANDMEMBER"
	SREM        = "SREM"
	SUNION      = "SUNION"
	SUNIONSTORE = "SUNIONSTORE"
	SSCAN       = "SSCAN"
)

func sadd(ctx context.Context, client Client, params []interface{}) (v int64, err errors.CodeError) {
	key := params[0].(string)
	members := make([]interface{}, 0, 1)
	for _, param := range params[1:] {
		members = append(members, param)
	}
	var doErr error
	v, doErr = client.Writer().SAdd(ctx, key, members...).Result()
	if doErr != nil {
		err = errors.ServiceError("redis: handle sadd command failed").WithCause(doErr)
		return
	}
	return
}

func smembers(ctx context.Context, client Client, params []interface{}) (v []string, err errors.CodeError) {
	key := params[0].(string)
	var doErr error
	v, doErr = client.Writer().SMembers(ctx, key).Result()
	if doErr != nil {
		err = errors.ServiceError("redis: handle smembers command failed").WithCause(doErr)
		return
	}
	return
}

func scard(ctx context.Context, client Client, key string) (v int64, err errors.CodeError) {
	var doErr error
	v, doErr = client.Reader().SCard(ctx, key).Result()
	if doErr != nil {
		if doErr == redis.Nil {
			err = errors.NotFound("redis: nil")
			return
		}
		err = errors.ServiceError("redis: handle scard command failed").WithCause(doErr)
		return
	}
	return
}

func sdiff(ctx context.Context, client Client, keys ...string) (v []string, err errors.CodeError) {
	var doErr error
	v, doErr = client.Reader().SDiff(ctx, keys...).Result()
	if doErr != nil {
		if doErr == redis.Nil {
			err = errors.NotFound("redis: nil")
			return
		}
		err = errors.ServiceError("redis: handle sdiff command failed").WithCause(doErr)
		return
	}
	return
}

func sdiffstore(ctx context.Context, client Client, destination string, keys ...string) (v int64, err errors.CodeError) {
	var doErr error
	v, doErr = client.Writer().SDiffStore(ctx, destination, keys...).Result()
	if doErr != nil {
		err = errors.ServiceError("redis: handle sdiffstore command failed").WithCause(doErr)
		return
	}
	return
}

func sinter(ctx context.Context, client Client, keys ...string) (v []string, err errors.CodeError) {
	var doErr error
	v, doErr = client.Reader().SInter(ctx, keys...).Result()
	if doErr != nil {
		if doErr == redis.Nil {
			err = errors.NotFound("redis: nil")
			return
		}
		err = errors.ServiceError("redis: handle sinter command failed").WithCause(doErr)
		return
	}
	return
}

func sinterstore(ctx context.Context, client Client, destination string, keys ...string) (v int64, err errors.CodeError) {
	var doErr error
	v, doErr = client.Writer().SInterStore(ctx, destination, keys...).Result()
	if doErr != nil {
		err = errors.ServiceError("redis: handle sinterstore command failed").WithCause(doErr)
		return
	}
	return
}

func sismember(ctx context.Context, client Client, key string, member interface{}) (v bool, err errors.CodeError) {
	var doErr error
	v, doErr = client.Writer().SIsMember(ctx, key, member).Result()
	if doErr != nil {
		err = errors.ServiceError("redis: handle sismember command failed").WithCause(doErr)
		return
	}
	return
}

func smove(ctx context.Context, client Client, source, destination string, member interface{}) (v bool, err errors.CodeError) {
	var doErr error
	v, doErr = client.Writer().SMove(ctx, source, destination, member).Result()
	if doErr != nil {
		err = errors.ServiceError("redis: handle smove command failed").WithCause(doErr)
		return
	}
	return
}

func spop(ctx context.Context, client Client, key string) (v string, err errors.CodeError) {
	var doErr error
	v, doErr = client.Writer().SPop(ctx, key).Result()
	if doErr != nil {
		err = errors.ServiceError("redis: handle spop command failed").WithCause(doErr)
		return
	}
	return
}

func srandmember(ctx context.Context, client Client, key string) (v string, err errors.CodeError) {
	var doErr error
	v, doErr = client.Reader().SRandMember(ctx, key).Result()
	if doErr != nil {
		if doErr == redis.Nil {
			err = errors.NotFound("redis: nil")
			return
		}
		err = errors.ServiceError("redis: handle srandmember command failed").WithCause(doErr)
		return
	}
	return
}

func srem(ctx context.Context, client Client, params []interface{}) (v int64, err errors.CodeError) {
	key := params[0].(string)
	members := make([]interface{}, 0, 1)
	for _, param := range params[1:] {
		members = append(members, param)
	}
	var doErr error
	v, doErr = client.Writer().SRem(ctx, key, members...).Result()
	if doErr != nil {
		err = errors.ServiceError("redis: handle srem command failed").WithCause(doErr)
		return
	}
	return
}

func sunion(ctx context.Context, client Client, keys ...string) (v []string, err errors.CodeError) {
	var doErr error
	v, doErr = client.Writer().SUnion(ctx, keys...).Result()
	if doErr != nil {
		err = errors.ServiceError("redis: handle sunion command failed").WithCause(doErr)
		return
	}
	return
}

func sunionstore(ctx context.Context, client Client, destination string, keys ...string) (v int64, err errors.CodeError) {
	var doErr error
	v, doErr = client.Writer().SUnionStore(ctx, destination, keys...).Result()
	if doErr != nil {
		err = errors.ServiceError("redis: handle sunionstore command failed").WithCause(doErr)
		return
	}
	return
}

func sscan(ctx context.Context, client Client, key string, cursor uint64, match string, count int64) (v []string, next uint64, err errors.CodeError) {
	var doErr error
	v, next, doErr = client.Reader().SScan(ctx, key, cursor, match, count).Result()
	if doErr != nil {
		if doErr == redis.Nil {
			err = errors.NotFound("redis: nil")
			return
		}
		err = errors.ServiceError("redis: handle sscan command failed").WithCause(doErr)
		return
	}
	return
}
