package shareds

import (
	"context"
	"fmt"
	"github.com/aacfactory/errors"
	"github.com/aacfactory/fns-contrib/databases/redis"
	"github.com/aacfactory/fns/commons/bytex"
	"github.com/aacfactory/fns/service/shared"
	"runtime"
	"time"
)

func Lockers() shared.Lockers {
	return &lockers{}
}

type lockers struct{}

func (l lockers) Acquire(ctx context.Context, key []byte, ttl time.Duration) (locker shared.Locker, err error) {
	id, incrErr := redis.Incr(ctx, fmt.Sprintf("fns/shared/lockers/%s", bytex.ToString(key)))
	if incrErr != nil {
		err = errors.Warning("lockers: acquire failed").WithCause(incrErr).WithMeta("kind", "redis")
		return
	}
	setErr := redis.Set(ctx, redis.SetParam{
		Key:        fmt.Sprintf("fns/shared/lockers/%s/%d", bytex.ToString(key), id),
		Value:      "1",
		Expiration: ttl,
	})
	if setErr != nil {
		err = errors.Warning("lockers: acquire failed").WithCause(setErr).WithMeta("kind", "redis")
		return
	}
	locker = &Locker{
		key: bytex.ToString(key),
		id:  id,
		ttl: ttl,
	}
	return
}

type Locker struct {
	key string
	id  int64
	ttl time.Duration
}

func (l *Locker) Lock(ctx context.Context) (err error) {
	times := 10
	deadline := time.Time{}
	if l.ttl > 0 {
		deadline = time.Now().Add(l.ttl)
	}
	ctxDeadline, hasCtxDeadline := ctx.Deadline()
	if hasCtxDeadline {
		if ctxDeadline.Before(deadline) || deadline.IsZero() {
			deadline = ctxDeadline
		}
	}
	rk := fmt.Sprintf("fns/shared/lockers/%s/%d", l.key, l.id)
	for {
		scanResult, scanErr := redis.Scan(ctx, redis.ScanParam{
			Cursor: 0,
			Match:  fmt.Sprintf("fns/shared/lockers/%s/*", l.key),
			Count:  1,
		})
		if scanErr != nil {
			err = errors.Warning("lockers: lock failed").
				WithCause(scanErr).
				WithMeta("kind", "redis").WithMeta("key", l.key)
			return
		}
		if scanResult.Keys == nil || len(scanResult.Keys) == 0 {
			break
		}
		prevKey := scanResult.Keys[0]
		if prevKey == rk {
			break
		}
		times--
		if times < 0 {
			if !deadline.IsZero() && time.Now().After(deadline) {
				err = errors.Warning("lockers: lock failed").
					WithCause(shared.ErrLockTimeout).
					WithMeta("kind", "redis").WithMeta("key", l.key)
				return
			}
			times = 10
			runtime.Gosched()
		}
	}
	return
}

func (l *Locker) Unlock(ctx context.Context) (err error) {
	_ = redis.Del(ctx, []string{fmt.Sprintf("fns/shared/lockers/%s/%d", l.key, l.id)})
	return
}
