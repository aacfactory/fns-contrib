package redis

import (
	"github.com/aacfactory/errors"
	"github.com/aacfactory/fns"
	"time"
)

func (svc *Service) contains(ctx fns.Context, key string) (ok bool, err errors.CodeError) {
	n, cmdErr := svc.client.Writer().Exists(ctx, key).Result()
	if cmdErr != nil {
		err = errors.ServiceError(cmdErr.Error())
		return
	}
	ok = n > 0
	return
}

func (svc *Service) remove(ctx fns.Context, key string) (ok bool, err errors.CodeError) {
	n, cmdErr := svc.client.Writer().Del(ctx, key).Result()
	if cmdErr != nil {
		err = errors.ServiceError(cmdErr.Error())
		return
	}
	ok = n > 0
	return
}

type ExpireParam struct {
	Key string        `json:"key,omitempty"`
	TTL time.Duration `json:"ttl,omitempty"`
}

func (svc *Service) expire(ctx fns.Context, param ExpireParam) (ok bool, err errors.CodeError) {
	n, cmdErr := svc.client.Writer().Expire(ctx, param.Key, param.TTL).Result()
	if cmdErr != nil {
		err = errors.ServiceError(cmdErr.Error())
		return
	}
	ok = n
	return
}

func (svc *Service) persist(ctx fns.Context, key string) (ok bool, err errors.CodeError) {
	n, cmdErr := svc.client.Writer().Persist(ctx, key).Result()
	if cmdErr != nil {
		err = errors.ServiceError(cmdErr.Error())
		return
	}
	ok = n
	return
}

func (svc *Service) ttl(ctx fns.Context, key string) (ttl time.Duration, err errors.CodeError) {
	n, cmdErr := svc.client.Reader().TTL(ctx, key).Result()
	if cmdErr != nil {
		err = errors.ServiceError(cmdErr.Error())
		return
	}
	ttl = n
	return
}
