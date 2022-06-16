package internal

import (
	"context"
	"github.com/aacfactory/errors"
	"github.com/go-redis/redis/v8"
)

const (
	ZADD             = "ZADD"
	ZCARD            = "ZCARD"
	ZCOUNT           = "ZCOUNT"
	ZINCRBY          = "ZINCRBY"
	ZINTERSTORE      = "ZINTERSTORE"
	ZLEXCOUNT        = "ZLEXCOUNT"
	ZRANGE           = "ZRANGE"
	ZRANGEBYLEX      = "ZRANGEBYLEX"
	ZRANGEBYSCORE    = "ZRANGEBYSCORE"
	ZRANK            = "ZRANK"
	ZREM             = "ZREM"
	ZREMRANGEBYLEX   = "ZREMRANGEBYLEX"
	ZREMRANGEBYRANK  = "ZREMRANGEBYRANK"
	ZREMRANGEBYSCORE = "ZREMRANGEBYSCORE"
	ZREVRANGE        = "ZREVRANGE"
	ZREVRANGEBYSCORE = "ZREVRANGEBYSCORE"
	ZREVRANK         = "ZREVRANK"
	ZSCORE           = "ZSCORE"
	ZUNIONSTORE      = "ZUNIONSTORE"
	ZSCAN            = "ZSCAN"
)

func zadd(ctx context.Context, client Client, key string, members ...*redis.Z) (v int64, err errors.CodeError) {
	var doErr error
	v, doErr = client.Writer().ZAdd(ctx, key, members...).Result()
	if doErr != nil {
		err = errors.ServiceError("redis: handle zadd command failed").WithCause(doErr)
		return
	}
	return
}

func zcard(ctx context.Context, client Client, key string) (v int64, err errors.CodeError) {
	var doErr error
	v, doErr = client.Reader().ZCard(ctx, key).Result()
	if doErr != nil {
		if doErr == redis.Nil {
			err = errors.NotFound("redis: nil")
			return
		}
		err = errors.ServiceError("redis: handle zcard command failed").WithCause(doErr)
		return
	}
	return
}

func zcount(ctx context.Context, client Client, key, min, max string) (v int64, err errors.CodeError) {
	var doErr error
	v, doErr = client.Reader().ZCount(ctx, key, min, max).Result()
	if doErr != nil {
		if doErr == redis.Nil {
			err = errors.NotFound("redis: nil")
			return
		}
		err = errors.ServiceError("redis: handle zcount command failed").WithCause(doErr)
		return
	}
	return
}

func zincrby(ctx context.Context, client Client, key string, increment float64, member string) (v float64, err errors.CodeError) {
	var doErr error
	v, doErr = client.Writer().ZIncrBy(ctx, key, increment, member).Result()
	if doErr != nil {
		err = errors.ServiceError("redis: handle zincrby command failed").WithCause(doErr)
		return
	}
	return
}

func zinterstore(ctx context.Context, client Client, destination string, store *redis.ZStore) (v int64, err errors.CodeError) {
	var doErr error
	v, doErr = client.Writer().ZInterStore(ctx, destination, store).Result()
	if doErr != nil {
		err = errors.ServiceError("redis: handle zinterstore command failed").WithCause(doErr)
		return
	}
	return
}

func zlexcount(ctx context.Context, client Client, key, min, max string) (v int64, err errors.CodeError) {
	var doErr error
	v, doErr = client.Reader().ZLexCount(ctx, key, min, max).Result()
	if doErr != nil {
		if doErr == redis.Nil {
			err = errors.NotFound("redis: nil")
			return
		}
		err = errors.ServiceError("redis: handle zlexcount command failed").WithCause(doErr)
		return
	}
	return
}

func zrange(ctx context.Context, client Client, key string, start, stop int64) (v []string, err errors.CodeError) {
	var doErr error
	v, doErr = client.Reader().ZRange(ctx, key, start, stop).Result()
	if doErr != nil {
		if doErr == redis.Nil {
			err = errors.NotFound("redis: nil")
			return
		}
		err = errors.ServiceError("redis: handle zrange command failed").WithCause(doErr)
		return
	}
	return
}

func zrangebylex(ctx context.Context, client Client, key string, opt *redis.ZRangeBy) (v []string, err errors.CodeError) {
	var doErr error
	v, doErr = client.Reader().ZRangeByLex(ctx, key, opt).Result()
	if doErr != nil {
		if doErr == redis.Nil {
			err = errors.NotFound("redis: nil")
			return
		}
		err = errors.ServiceError("redis: handle zrangebylex command failed").WithCause(doErr)
		return
	}
	return
}

func zrangebyscore(ctx context.Context, client Client, key string, opt *redis.ZRangeBy) (v []string, err errors.CodeError) {
	var doErr error
	v, doErr = client.Reader().ZRangeByScore(ctx, key, opt).Result()
	if doErr != nil {
		if doErr == redis.Nil {
			err = errors.NotFound("redis: nil")
			return
		}
		err = errors.ServiceError("redis: handle zrangebyscore command failed").WithCause(doErr)
		return
	}
	return
}

func zrank(ctx context.Context, client Client, key, member string) (v int64, err errors.CodeError) {
	var doErr error
	v, doErr = client.Writer().ZRank(ctx, key, member).Result()
	if doErr != nil {
		err = errors.ServiceError("redis: handle zrank command failed").WithCause(doErr)
		return
	}
	return
}

func zrem(ctx context.Context, client Client, key string, member ...interface{}) (v int64, err errors.CodeError) {
	var doErr error
	v, doErr = client.Writer().ZRem(ctx, key, member...).Result()
	if doErr != nil {
		err = errors.ServiceError("redis: handle zrem command failed").WithCause(doErr)
		return
	}
	return
}

func zremrangebylex(ctx context.Context, client Client, key, min, max string) (v int64, err errors.CodeError) {
	var doErr error
	v, doErr = client.Writer().ZRemRangeByLex(ctx, key, min, max).Result()
	if doErr != nil {
		err = errors.ServiceError("redis: handle zremrangebylex command failed").WithCause(doErr)
		return
	}
	return
}

func zremrangebyrank(ctx context.Context, client Client, key string, start, stop int64) (v int64, err errors.CodeError) {
	var doErr error
	v, doErr = client.Writer().ZRemRangeByRank(ctx, key, start, stop).Result()
	if doErr != nil {
		err = errors.ServiceError("redis: handle zremrangebyrank command failed").WithCause(doErr)
		return
	}
	return
}

func zremrangebyscore(ctx context.Context, client Client, key string, min, max string) (v int64, err errors.CodeError) {
	var doErr error
	v, doErr = client.Writer().ZRemRangeByScore(ctx, key, min, max).Result()
	if doErr != nil {
		err = errors.ServiceError("redis: handle zremrangebyscore command failed").WithCause(doErr)
		return
	}
	return
}

func zrevrnage(ctx context.Context, client Client, key string, start, stop int64) (v []string, err errors.CodeError) {
	var doErr error
	v, doErr = client.Reader().ZRevRange(ctx, key, start, stop).Result()
	if doErr != nil {
		if doErr == redis.Nil {
			err = errors.NotFound("redis: nil")
			return
		}
		err = errors.ServiceError("redis: handle zrevrnage command failed").WithCause(doErr)
		return
	}
	return
}

func zrevrnagebyscore(ctx context.Context, client Client, key string, opt *redis.ZRangeBy) (v []string, err errors.CodeError) {
	var doErr error
	v, doErr = client.Reader().ZRevRangeByScore(ctx, key, opt).Result()
	if doErr != nil {
		if doErr == redis.Nil {
			err = errors.NotFound("redis: nil")
			return
		}
		err = errors.ServiceError("redis: handle zrevrnagebyscore command failed").WithCause(doErr)
		return
	}
	return
}

func zrevrank(ctx context.Context, client Client, key string, start, stop int64) (v []string, err errors.CodeError) {
	var doErr error
	v, doErr = client.Reader().ZRevRange(ctx, key, start, stop).Result()
	if doErr != nil {
		if doErr == redis.Nil {
			err = errors.NotFound("redis: nil")
			return
		}
		err = errors.ServiceError("redis: handle zrevrank command failed").WithCause(doErr)
		return
	}
	return
}

func zscore(ctx context.Context, client Client, key, member string) (v float64, err errors.CodeError) {
	var doErr error
	v, doErr = client.Reader().ZScore(ctx, key, member).Result()
	if doErr != nil {
		if doErr == redis.Nil {
			err = errors.NotFound("redis: nil")
			return
		}
		err = errors.ServiceError("redis: handle zscore command failed").WithCause(doErr)
		return
	}
	return
}

func zunionstore(ctx context.Context, client Client, dest string, store *redis.ZStore) (v int64, err errors.CodeError) {
	var doErr error
	v, doErr = client.Writer().ZUnionStore(ctx, dest, store).Result()
	if doErr != nil {
		err = errors.ServiceError("redis: handle zunionstore command failed").WithCause(doErr)
		return
	}
	return
}

func zscan(ctx context.Context, client Client, key string, cursor uint64, match string, count int64) (v []string, next uint64, err errors.CodeError) {
	var doErr error
	v, next, doErr = client.Reader().ZScan(ctx, key, cursor, match, count).Result()
	if doErr != nil {
		if doErr == redis.Nil {
			err = errors.NotFound("redis: nil")
			return
		}
		err = errors.ServiceError("redis: handle zscan command failed").WithCause(doErr)
		return
	}
	return
}
