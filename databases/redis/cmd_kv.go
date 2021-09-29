package redis

import (
	"github.com/aacfactory/errors"
	"github.com/aacfactory/fns"
	"github.com/aacfactory/json"
	rds "github.com/go-redis/redis/v8"
	"time"
)

type SetParam struct {
	Key        string          `json:"key,omitempty"`
	Value      json.RawMessage `json:"value,omitempty"`
	Expiration time.Duration   `json:"expiration,omitempty"`
}

func (svc *_service) set(ctx fns.Context, param SetParam) (err errors.CodeError) {
	cmdErr := svc.client.Writer().Set(ctx, param.Key, string(param.Value), param.Expiration).Err()
	if cmdErr != nil {
		err = errors.ServiceError(cmdErr.Error())
		return
	}
	return
}

type GetResult struct {
	Value json.RawMessage `json:"value,omitempty"`
	Has   bool            `json:"has,omitempty"`
}

func (svc *_service) get(ctx fns.Context, key string) (result *GetResult, err errors.CodeError) {
	v, cmdErr := svc.client.Reader().Get(ctx, key).Result()
	if cmdErr != nil {
		if cmdErr == rds.Nil {
			result = &GetResult{
				Value: nil,
				Has:   false,
			}
			return
		}
		err = errors.ServiceError(cmdErr.Error())
		return
	}
	result = &GetResult{
		Value: []byte(v),
		Has:   true,
	}
	return
}

func (svc *_service) getAndSet(ctx fns.Context, param SetParam) (result *GetResult, err errors.CodeError) {
	v, cmdErr := svc.client.Writer().GetSet(ctx, param.Key, string(param.Value)).Result()
	if cmdErr != nil {
		if cmdErr == rds.Nil {
			result = &GetResult{
				Value: nil,
				Has:   false,
			}
			_, _ = svc.expire(ctx, ExpireParam{
				Key: param.Key,
				TTL: param.Expiration,
			})
			return
		}
		err = errors.ServiceError(cmdErr.Error())
		return
	}
	result = &GetResult{
		Value: []byte(v),
		Has:   true,
	}
	_, _ = svc.expire(ctx, ExpireParam{
		Key: param.Key,
		TTL: param.Expiration,
	})
	return
}

type atomicResult struct {
	Value int64 `json:"value,omitempty"`
}

func (svc *_service) incr(ctx fns.Context, key string) (result *atomicResult, err errors.CodeError) {
	if svc.client.Writer().Exists(ctx, key).Val() == 0 {
		svc.client.Writer().SetNX(ctx, key, int64(0), 0)
	}
	x, cmdErr := svc.client.Writer().Incr(ctx, key).Result()
	if cmdErr != nil {
		err = errors.ServiceError(cmdErr.Error())
		return
	}
	result = &atomicResult{
		Value: x,
	}
	return
}

func (svc *_service) decr(ctx fns.Context, key string) (result *atomicResult, err errors.CodeError) {
	if svc.client.Writer().Exists(ctx, key).Val() == 0 {
		svc.client.Writer().SetNX(ctx, key, int64(0), 0)
	}

	x, cmdErr := svc.client.Writer().Decr(ctx, key).Result()
	if cmdErr != nil {
		err = errors.ServiceError(cmdErr.Error())
		return
	}
	result = &atomicResult{
		Value: x,
	}
	return
}
