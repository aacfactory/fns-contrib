package internal

import (
	"context"
	"github.com/aacfactory/errors"
	"github.com/go-redis/redis/v8"
)

const (
	SORT = "SORT"
)

func sort(ctx context.Context, client Client, key string, opt *redis.Sort) (v []string, err errors.CodeError) {
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
