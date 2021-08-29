package redis

import (
	"github.com/aacfactory/errors"
	"github.com/aacfactory/fns"
	"github.com/aacfactory/json"
	"time"
)

type SetParam struct {
	Key        string          `json:"key,omitempty"`
	Value      json.RawMessage `json:"value,omitempty"`
	Expiration time.Duration   `json:"expiration,omitempty"`
}

func (svc *Service) set(ctx fns.Context, param SetParam) (err errors.CodeError) {
	cmdErr := svc.client.Writer().Set(ctx, param.Key, param.Value, param.Expiration).Err()
	if cmdErr != nil {
		err = errors.ServiceError(cmdErr.Error())
		return
	}
	return
}

func (svc *Service) get(ctx fns.Context, key string) (result json.RawMessage, err errors.CodeError) {
	v, cmdErr := svc.client.Reader().Get(ctx, key).Result()
	if cmdErr != nil {
		err = errors.ServiceError(cmdErr.Error())
		return
	}
	result = []byte(v)
	return
}

func (svc *Service) incr(ctx fns.Context, key string) (err errors.CodeError) {
	if svc.client.Writer().Exists(ctx, key).Val() == 0 {
		svc.client.Writer().SetNX(ctx, key, int64(0), 0)
	}

	cmdErr := svc.client.Writer().Incr(ctx, key).Err()
	if cmdErr != nil {
		err = errors.ServiceError(cmdErr.Error())
		return
	}
	return
}

func (svc *Service) decr(ctx fns.Context, key string) (err errors.CodeError) {
	if svc.client.Writer().Exists(ctx, key).Val() == 0 {
		svc.client.Writer().SetNX(ctx, key, int64(0), 0)
	}

	cmdErr := svc.client.Writer().Decr(ctx, key).Err()
	if cmdErr != nil {
		err = errors.ServiceError(cmdErr.Error())
		return
	}
	return
}
