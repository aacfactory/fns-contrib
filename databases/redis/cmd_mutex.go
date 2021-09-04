package redis

import (
	"github.com/aacfactory/errors"
	"github.com/aacfactory/fns"
	"time"
)

type LockParam struct {
	Key string        `json:"key,omitempty"`
	TTL time.Duration `json:"ttl,omitempty"`
}

func (svc *_service) lock(ctx fns.Context, param LockParam) (err errors.CodeError) {

	ttl := param.TTL
	if ttl < time.Second {
		ttl = 10 * time.Second
	}

	id := fns.UID()

	pushErr := svc.client.Writer().RPush(ctx, param.Key, id).Err()
	if pushErr != nil {
		err = errors.ServiceError("fns Redis: lock failed").WithCause(pushErr)
		return
	}

	expireErr := svc.client.Writer().Expire(ctx, param.Key, ttl).Err()
	if expireErr != nil {
		err = errors.ServiceError("fns Redis: lock failed").WithCause(expireErr)
		return
	}

	for {
		head, getErr := svc.client.Writer().LRange(ctx, param.Key, 0, 0).Result()
		if getErr != nil {
			err = errors.ServiceError("fns Redis: lock failed").WithCause(getErr)
			return
		}
		if head == nil || len(head) == 0 {
			err = errors.ServiceError("fns Redis: lock failed for no entry in lock list")
			return
		}
		if head[0] == id {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}

	return
}

func (svc *_service) unlock(ctx fns.Context, key string) (err errors.CodeError) {

	popErr := svc.client.Writer().LPop(ctx, key).Err()
	if popErr != nil {
		err = errors.ServiceError("fns Redis: unlock failed").WithCause(popErr)
		return
	}

	return
}
