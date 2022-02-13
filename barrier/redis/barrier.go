package redis

import (
	"fmt"
	"github.com/aacfactory/errors"
	"github.com/aacfactory/fns"
	rds "github.com/aacfactory/fns-contrib/databases/redis"
	"github.com/aacfactory/json"
	"time"
)

func init() {
	fns.RegisterServiceBarrierRetriever(ServiceBarrierRetriever)
}

func ServiceBarrierRetriever() (b fns.ServiceBarrier) {
	return &serviceBarrier{}
}

type serviceBarrierExecuteKey struct {
	Key string `json:"key"`
}

type serviceBarrierExecuteResult struct {
	Value   json.RawMessage `json:"value"`
	Succeed bool            `json:"succeed"`
}

type serviceBarrier struct {
}

func (b *serviceBarrier) makeKey(key string) string {
	return fmt.Sprintf("fns_barrier_%s", key)
}

func (b *serviceBarrier) Do(ctx fns.Context, key string, fn func() (v interface{}, err error)) (v interface{}, err error, shared bool) {
	execCacheKey := b.makeKey(key)
	execKey := &serviceBarrierExecuteKey{
		Key: fmt.Sprintf("fns_barrier_r_%s", key),
	}
	execKeyBytes, _ := json.Marshal(execKey)
	getResult, gsErr := rds.GetSet(ctx, rds.SetParam{
		Key:        execCacheKey,
		Value:      execKeyBytes,
		Expiration: 10 * time.Second,
	})
	if gsErr != nil {
		err = errors.ServiceError("fns barrier: request has be blocked by service barrier cause init failed").WithCause(gsErr)
		return
	}
	if getResult.Has {
		// 不是第一次
		for i := 0; i < 10; i++ {
			execResultCache, getExecResult := rds.Get(ctx, execKey.Key)
			if getExecResult != nil {
				err = errors.ServiceError("fns barrier: request has be blocked by service barrier cause get result failed").WithCause(getExecResult)
				break
			}
			if !execResultCache.Has {
				// no fn result
				time.Sleep(50 * time.Millisecond)
				continue
			}
			execResult := &serviceBarrierExecuteResult{}
			decodeErr := json.Unmarshal(execResultCache.Value, execResult)
			if decodeErr != nil {
				err = errors.ServiceError("fns barrier: request has be blocked by service barrier cause decode result failed").WithCause(decodeErr)
				break
			}
			shared = true
			if execResult.Succeed {
				v = execResult.Value
			} else {
				fnErr := errors.Warning("").(errors.CodeError)
				decodeResultFailedErr := json.Unmarshal(execResult.Value, fnErr)
				if decodeResultFailedErr != nil {
					err = errors.ServiceError("fns barrier: request has be blocked by service barrier cause decode result cause failed").WithCause(decodeResultFailedErr)
					break
				}
				err = fnErr
			}
			break
		}
		if v == nil && err == nil {
			v, err, shared = b.Do(ctx, key, fn)
			return
		}
		return
	}
	// clean
	_ = rds.Remove(ctx, execKey.Key)
	// execute
	v, err = fn()
	execResult := &serviceBarrierExecuteResult{}
	if err != nil {
		execResult.Succeed = false
		execResult.Value, _ = json.Marshal(err)
	} else {
		execResult.Succeed = true
		execResult.Value, _ = json.Marshal(v)
	}
	execResultBytes, _ := json.Marshal(execResult)
	setErr := rds.Set(ctx, rds.SetParam{
		Key:        execKey.Key,
		Value:      execResultBytes,
		Expiration: 10 * time.Second,
	})
	if setErr != nil {
		ctx.App().Log().Warn().Cause(setErr).Message("fns barrier: service barrier set result failed")
	}
	return
}

func (b *serviceBarrier) Forget(ctx fns.Context, key string) {
	rmErr := rds.Remove(ctx, b.makeKey(key))
	if rmErr != nil {
		ctx.App().Log().Warn().Cause(rmErr).Message("fns barrier: service barrier remove result failed")
	}
}
