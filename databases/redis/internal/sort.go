package internal

import (
	"context"
	"github.com/aacfactory/errors"
	"github.com/go-redis/redis/v8"
)

const (
	SORT = "SORT"
)

func sort(ctx context.Context, client Client, params []interface{}) (v []string, err errors.CodeError) {
	key := params[0].(string)
	opt := &redis.Sort{}
	params = params[1:]
	for i := 0; i < len(params); i++ {
		on := params[i].(string)
		switch on {
		case "by":
			opt.By = params[i+1].(string)
			i++
		case "limit":
			opt.Offset = params[i+1].(int64)
			i++
			opt.Count = params[i+1].(int64)
			i++
		case "get":
			opt.Get = params[i+1].([]string)
			i++
		case "alpha":
			opt.Alpha = true
		default:
			opt.Order = params[i].(string)
			return
		}
	}
	var doErr error
	v, doErr = client.Reader().Sort(ctx, key, opt).Result()
	if doErr != nil {
		err = errors.ServiceError("redis: handle sort command failed").WithCause(doErr)
		return
	}
	return
}

func sortInterfaces(ctx context.Context, client Client, key string, opt *redis.Sort) (v []interface{}, err errors.CodeError) {
	var doErr error
	v, doErr = client.Reader().SortInterfaces(ctx, key, opt).Result()
	if doErr != nil {
		err = errors.ServiceError("redis: handle sortinterfaces command failed").WithCause(doErr)
		return
	}
	return
}
