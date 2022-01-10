package redis

import (
	"fmt"
	"github.com/aacfactory/errors"
	"github.com/aacfactory/fns"
	"github.com/aacfactory/json"
	"time"
)

func Do(ctx fns.Context, param OriginCommandArg) (result OriginCommandResult, err errors.CodeError) {
	proxy, proxyErr := ctx.App().ServiceProxy(ctx, namespace)
	if proxyErr != nil {
		err = errors.New(555, "***WARNING***", fmt.Sprintf("fns Redis Proxy: get %s proxy failed", namespace)).WithCause(proxyErr)
		return
	}
	arg, argErr := fns.NewArgument(param)
	if argErr != nil {
		err = argErr
		return
	}

	r := proxy.Request(ctx, originCmdFn, arg)
	result = OriginCommandResult{}
	err = r.Get(ctx, &result)
	return
}

func Contains(ctx fns.Context, key string) (ok bool, err errors.CodeError) {
	proxy, proxyErr := ctx.App().ServiceProxy(ctx, namespace)
	if proxyErr != nil {
		err = errors.New(555, "***WARNING***", fmt.Sprintf("fns Redis Proxy: get %s proxy failed", namespace)).WithCause(proxyErr)
		return
	}
	arg, argErr := fns.NewArgument(key)
	if argErr != nil {
		err = argErr
		return
	}

	r := proxy.Request(ctx, containsFn, arg)
	err = r.Get(ctx, &ok)
	return
}

func Remove(ctx fns.Context, key string) (err errors.CodeError) {
	proxy, proxyErr := ctx.App().ServiceProxy(ctx, namespace)
	if proxyErr != nil {
		err = errors.New(555, "***WARNING***", fmt.Sprintf("fns Redis Proxy: get %s proxy failed", namespace)).WithCause(proxyErr)
		return
	}
	arg, argErr := fns.NewArgument(key)
	if argErr != nil {
		err = argErr
		return
	}

	r := proxy.Request(ctx, removeFn, arg)
	err = r.Get(ctx, &json.RawMessage{})
	return
}

func Expire(ctx fns.Context, param ExpireParam) (ok bool, err errors.CodeError) {
	proxy, proxyErr := ctx.App().ServiceProxy(ctx, namespace)
	if proxyErr != nil {
		err = errors.New(555, "***WARNING***", fmt.Sprintf("fns Redis Proxy: get %s proxy failed", namespace)).WithCause(proxyErr)
		return
	}
	arg, argErr := fns.NewArgument(param)
	if argErr != nil {
		err = argErr
		return
	}

	r := proxy.Request(ctx, expireFn, arg)
	err = r.Get(ctx, &ok)
	return
}

func Persist(ctx fns.Context, key string) (ok bool, err errors.CodeError) {
	proxy, proxyErr := ctx.App().ServiceProxy(ctx, namespace)
	if proxyErr != nil {
		err = errors.New(555, "***WARNING***", fmt.Sprintf("fns Redis Proxy: get %s proxy failed", namespace)).WithCause(proxyErr)
		return
	}
	arg, argErr := fns.NewArgument(key)
	if argErr != nil {
		err = argErr
		return
	}

	r := proxy.Request(ctx, persistFn, arg)
	err = r.Get(ctx, &ok)
	return
}

func TTL(ctx fns.Context, key string) (ttl time.Duration, err errors.CodeError) {
	proxy, proxyErr := ctx.App().ServiceProxy(ctx, namespace)
	if proxyErr != nil {
		err = errors.New(555, "***WARNING***", fmt.Sprintf("fns Redis Proxy: get %s proxy failed", namespace)).WithCause(proxyErr)
		return
	}
	arg, argErr := fns.NewArgument(key)
	if argErr != nil {
		err = argErr
		return
	}

	r := proxy.Request(ctx, ttlFn, arg)
	err = r.Get(ctx, &ttl)
	return
}

func Set(ctx fns.Context, param SetParam) (err errors.CodeError) {
	proxy, proxyErr := ctx.App().ServiceProxy(ctx, namespace)
	if proxyErr != nil {
		err = errors.New(555, "***WARNING***", fmt.Sprintf("fns Redis Proxy: get %s proxy failed", namespace)).WithCause(proxyErr)
		return
	}
	arg, argErr := fns.NewArgument(param)
	if argErr != nil {
		err = argErr
		return
	}

	r := proxy.Request(ctx, setFn, arg)
	err = r.Get(ctx, &json.RawMessage{})
	return
}

func Get(ctx fns.Context, key string) (result *GetResult, err errors.CodeError) {
	proxy, proxyErr := ctx.App().ServiceProxy(ctx, namespace)
	if proxyErr != nil {
		err = errors.New(555, "***WARNING***", fmt.Sprintf("fns Redis Proxy: get %s proxy failed", namespace)).WithCause(proxyErr)
		return
	}
	arg, argErr := fns.NewArgument(key)
	if argErr != nil {
		err = argErr
		return
	}
	result = &GetResult{}
	r := proxy.Request(ctx, getFn, arg)
	err = r.Get(ctx, result)
	return
}

func GetSet(ctx fns.Context, param SetParam) (result *GetResult, err errors.CodeError) {
	proxy, proxyErr := ctx.App().ServiceProxy(ctx, namespace)
	if proxyErr != nil {
		err = errors.New(555, "***WARNING***", fmt.Sprintf("fns Redis Proxy: get %s proxy failed", namespace)).WithCause(proxyErr)
		return
	}
	arg, argErr := fns.NewArgument(param)
	if argErr != nil {
		err = argErr
		return
	}
	result = &GetResult{}
	r := proxy.Request(ctx, getSetFn, arg)
	err = r.Get(ctx, result)
	return
}

func Incr(ctx fns.Context, key string) (v int64, err errors.CodeError) {
	proxy, proxyErr := ctx.App().ServiceProxy(ctx, namespace)
	if proxyErr != nil {
		err = errors.New(555, "***WARNING***", fmt.Sprintf("fns Redis Proxy: get %s proxy failed", namespace)).WithCause(proxyErr)
		return
	}
	arg, argErr := fns.NewArgument(key)
	if argErr != nil {
		err = argErr
		return
	}
	r := proxy.Request(ctx, incrFn, arg)
	x := atomicResult{}
	err = r.Get(ctx, &x)
	v = x.Value
	return
}

func Decr(ctx fns.Context, key string) (v int64, err errors.CodeError) {
	proxy, proxyErr := ctx.App().ServiceProxy(ctx, namespace)
	if proxyErr != nil {
		err = errors.New(555, "***WARNING***", fmt.Sprintf("fns Redis Proxy: get %s proxy failed", namespace)).WithCause(proxyErr)
		return
	}
	arg, argErr := fns.NewArgument(key)
	if argErr != nil {
		err = argErr
		return
	}
	r := proxy.Request(ctx, decrFn, arg)
	x := atomicResult{}
	err = r.Get(ctx, &x)
	v = x.Value
	return
}

func Lock(ctx fns.Context, param LockParam) (err errors.CodeError) {
	proxy, proxyErr := ctx.App().ServiceProxy(ctx, namespace)
	if proxyErr != nil {
		err = errors.New(555, "***WARNING***", fmt.Sprintf("fns Redis Proxy: get %s proxy failed", namespace)).WithCause(proxyErr)
		return
	}
	arg, argErr := fns.NewArgument(param)
	if argErr != nil {
		err = argErr
		return
	}
	r := proxy.Request(ctx, lockFn, arg)
	err = r.Get(ctx, &json.RawMessage{})
	return
}

func Unlock(ctx fns.Context, key string) (err errors.CodeError) {
	proxy, proxyErr := ctx.App().ServiceProxy(ctx, namespace)
	if proxyErr != nil {
		err = errors.New(555, "***WARNING***", fmt.Sprintf("fns Redis Proxy: get %s proxy failed", namespace)).WithCause(proxyErr)
		return
	}
	arg, argErr := fns.NewArgument(key)
	if argErr != nil {
		err = argErr
		return
	}
	r := proxy.Request(ctx, unlockFn, arg)
	err = r.Get(ctx, &json.RawMessage{})
	return
}

func ZAdd(ctx fns.Context, param ZAddParam) (err errors.CodeError) {
	proxy, proxyErr := ctx.App().ServiceProxy(ctx, namespace)
	if proxyErr != nil {
		err = errors.New(555, "***WARNING***", fmt.Sprintf("fns Redis Proxy: get %s proxy failed", namespace)).WithCause(proxyErr)
		return
	}
	arg, argErr := fns.NewArgument(param)
	if argErr != nil {
		err = argErr
		return
	}
	r := proxy.Request(ctx, zAddFn, arg)
	err = r.Get(ctx, &json.RawMessage{})
	return
}

func ZCard(ctx fns.Context, key string) (num int64, err errors.CodeError) {
	proxy, proxyErr := ctx.App().ServiceProxy(ctx, namespace)
	if proxyErr != nil {
		err = errors.New(555, "***WARNING***", fmt.Sprintf("fns Redis Proxy: get %s proxy failed", namespace)).WithCause(proxyErr)
		return
	}
	arg, argErr := fns.NewArgument(key)
	if argErr != nil {
		err = argErr
		return
	}
	r := proxy.Request(ctx, zCardFn, arg)
	err = r.Get(ctx, &num)
	return
}

func ZRange(ctx fns.Context, param ZRangeParam) (result *json.Array, err errors.CodeError) {
	proxy, proxyErr := ctx.App().ServiceProxy(ctx, namespace)
	if proxyErr != nil {
		err = errors.New(555, "***WARNING***", fmt.Sprintf("fns Redis Proxy: get %s proxy failed", namespace)).WithCause(proxyErr)
		return
	}
	arg, argErr := fns.NewArgument(param)
	if argErr != nil {
		err = argErr
		return
	}
	result = json.NewArray()
	r := proxy.Request(ctx, zRangeFn, arg)
	err = r.Get(ctx, &result)
	return
}

func zRangeByScore(ctx fns.Context, param ZRangeByScoreParam) (result *json.Array, err errors.CodeError) {
	proxy, proxyErr := ctx.App().ServiceProxy(ctx, namespace)
	if proxyErr != nil {
		err = errors.New(555, "***WARNING***", fmt.Sprintf("fns Redis Proxy: get %s proxy failed", namespace)).WithCause(proxyErr)
		return
	}
	arg, argErr := fns.NewArgument(param)
	if argErr != nil {
		err = argErr
		return
	}
	result = json.NewArray()
	r := proxy.Request(ctx, zRangeByScoreFn, arg)
	err = r.Get(ctx, &result)
	return
}

func ZRem(ctx fns.Context, param ZRemParam) (ok bool, err errors.CodeError) {
	proxy, proxyErr := ctx.App().ServiceProxy(ctx, namespace)
	if proxyErr != nil {
		err = errors.New(555, "***WARNING***", fmt.Sprintf("fns Redis Proxy: get %s proxy failed", namespace)).WithCause(proxyErr)
		return
	}
	arg, argErr := fns.NewArgument(param)
	if argErr != nil {
		err = argErr
		return
	}
	r := proxy.Request(ctx, zRemFn, arg)
	err = r.Get(ctx, &ok)
	return
}

func ZRemByRank(ctx fns.Context, param ZRemByRankParam) (ok bool, err errors.CodeError) {
	proxy, proxyErr := ctx.App().ServiceProxy(ctx, namespace)
	if proxyErr != nil {
		err = errors.New(555, "***WARNING***", fmt.Sprintf("fns Redis Proxy: get %s proxy failed", namespace)).WithCause(proxyErr)
		return
	}
	arg, argErr := fns.NewArgument(param)
	if argErr != nil {
		err = argErr
		return
	}
	r := proxy.Request(ctx, zRemByRankFn, arg)
	err = r.Get(ctx, &ok)
	return
}

func ZRemByScore(ctx fns.Context, param ZRemByScoreParam) (ok bool, err errors.CodeError) {
	proxy, proxyErr := ctx.App().ServiceProxy(ctx, namespace)
	if proxyErr != nil {
		err = errors.New(555, "***WARNING***", fmt.Sprintf("fns Redis Proxy: get %s proxy failed", namespace)).WithCause(proxyErr)
		return
	}
	arg, argErr := fns.NewArgument(param)
	if argErr != nil {
		err = argErr
		return
	}
	r := proxy.Request(ctx, zRemByScoreFn, arg)
	err = r.Get(ctx, &ok)
	return
}

func ZRevRange(ctx fns.Context, param ZRevRangeParam) (result *json.Array, err errors.CodeError) {
	proxy, proxyErr := ctx.App().ServiceProxy(ctx, namespace)
	if proxyErr != nil {
		err = errors.New(555, "***WARNING***", fmt.Sprintf("fns Redis Proxy: get %s proxy failed", namespace)).WithCause(proxyErr)
		return
	}
	arg, argErr := fns.NewArgument(param)
	if argErr != nil {
		err = argErr
		return
	}
	result = json.NewArray()
	r := proxy.Request(ctx, zRevRangeFn, arg)
	err = r.Get(ctx, &result)
	return
}

func ZRevRangeByScore(ctx fns.Context, param ZRevRangeByScoreParam) (result *json.Array, err errors.CodeError) {
	proxy, proxyErr := ctx.App().ServiceProxy(ctx, namespace)
	if proxyErr != nil {
		err = errors.New(555, "***WARNING***", fmt.Sprintf("fns Redis Proxy: get %s proxy failed", namespace)).WithCause(proxyErr)
		return
	}
	arg, argErr := fns.NewArgument(param)
	if argErr != nil {
		err = argErr
		return
	}
	result = json.NewArray()
	r := proxy.Request(ctx, zRevRangeByScoreFn, arg)
	err = r.Get(ctx, &result)
	return
}
