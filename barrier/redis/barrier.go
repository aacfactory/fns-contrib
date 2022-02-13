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
	fns.RegisterServiceBarrierRetriever("remote", ServiceBarrierRetriever)
}

func ServiceBarrierRetriever() (b fns.ServiceBarrier) {
	return &serviceBarrier{}
}

type serviceBarrierExecuteResult struct {
	Has     bool            `json:"has"`
	Value   json.RawMessage `json:"value"`
	Succeed bool            `json:"succeed"`
}

type serviceBarrier struct {
}

func (b *serviceBarrier) makeKey(key string) string {
	return fmt.Sprintf("fn_b_%s", key)
}

func (b *serviceBarrier) Do(ctx fns.Context, key string, fn func() (v interface{}, err error)) (v interface{}, err error, shared bool) {
	key = b.makeKey(key)

	execResult := &serviceBarrierExecuteResult{}
	execResultBytes, _ := json.Marshal(execResult)
	getResult, gsErr := rds.GetSet(ctx, rds.SetParam{
		Key:        key,
		Value:      execResultBytes,
		Expiration: 10 * time.Second,
	})
	if gsErr != nil {
		err = errors.ServiceError("fns barrier: request has be blocked by service barrier cause init failed").WithCause(gsErr)
		return
	}
	if getResult.Has {
		for i := 0; i < 5; i++ {
			decodeErr := json.Unmarshal(getResult.Value, execResult)
			if decodeErr != nil {
				err = errors.ServiceError("fns barrier: request has be blocked by service barrier cause result failed").WithCause(decodeErr)
				break
			}
			if execResult.Has {
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
			time.Sleep(300 * time.Millisecond)
			getResult, gsErr = rds.Get(ctx, key)
			if gsErr != nil {
				err = errors.ServiceError("fns barrier: request has be blocked by service barrier cause get shared failed").WithCause(gsErr)
				break
			}
			if !getResult.Has {
				err = errors.ServiceError("fns barrier: request has be blocked by service barrier cause timeout")
				break
			}
		}
		if v == nil && err == nil {
			err = errors.ServiceError("fns barrier: request has be blocked by service barrier cause request is duplicated")
			return
		}
	}
	// execute
	v, err = fn()
	execResult.Has = true
	if err != nil {
		execResult.Succeed = false
		execResult.Value, _ = json.Marshal(err)
	} else {
		execResult.Succeed = true
		execResult.Value, _ = json.Marshal(v)
	}
	execResultBytes, _ = json.Marshal(execResult)
	setErr := rds.Set(ctx, rds.SetParam{
		Key:        key,
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
