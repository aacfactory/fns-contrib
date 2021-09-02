package redis

import (
	"fmt"
	"github.com/aacfactory/errors"
	"github.com/aacfactory/fns"
	"github.com/aacfactory/json"
	"time"
)

func Do(ctx fns.Context, param OriginCommandArg) (result OriginCommandResult, err errors.CodeError) {
	proxy, proxyErr := ctx.App().ServiceProxy(ctx, Namespace)
	if proxyErr != nil {
		err = errors.New(555, "***WARNING***", fmt.Sprintf("fns Redis Proxy: get %s proxy failed", Namespace)).WithCause(proxyErr)
		return
	}
	arg, argErr := fns.NewArgument(param)
	if argErr != nil {
		err = argErr
		return
	}

	r := proxy.Request(ctx, OriginCmdFn, arg)
	result = OriginCommandResult{}
	err = r.Get(ctx, &result)
	return
}

func Contains(ctx fns.Context, key string) (ok bool, err errors.CodeError) {
	proxy, proxyErr := ctx.App().ServiceProxy(ctx, Namespace)
	if proxyErr != nil {
		err = errors.New(555, "***WARNING***", fmt.Sprintf("fns Redis Proxy: get %s proxy failed", Namespace)).WithCause(proxyErr)
		return
	}
	arg, argErr := fns.NewArgument(key)
	if argErr != nil {
		err = argErr
		return
	}

	r := proxy.Request(ctx, ContainsFn, arg)
	err = r.Get(ctx, &ok)
	return
}

func Remove(ctx fns.Context, key string) (ok bool, err errors.CodeError) {
	proxy, proxyErr := ctx.App().ServiceProxy(ctx, Namespace)
	if proxyErr != nil {
		err = errors.New(555, "***WARNING***", fmt.Sprintf("fns Redis Proxy: get %s proxy failed", Namespace)).WithCause(proxyErr)
		return
	}
	arg, argErr := fns.NewArgument(key)
	if argErr != nil {
		err = argErr
		return
	}

	r := proxy.Request(ctx, RemoveFn, arg)
	err = r.Get(ctx, &ok)
	return
}

func Expire(ctx fns.Context, param ExpireParam) (ok bool, err errors.CodeError) {
	proxy, proxyErr := ctx.App().ServiceProxy(ctx, Namespace)
	if proxyErr != nil {
		err = errors.New(555, "***WARNING***", fmt.Sprintf("fns Redis Proxy: get %s proxy failed", Namespace)).WithCause(proxyErr)
		return
	}
	arg, argErr := fns.NewArgument(param)
	if argErr != nil {
		err = argErr
		return
	}

	r := proxy.Request(ctx, ExpireFn, arg)
	err = r.Get(ctx, &ok)
	return
}

func Persist(ctx fns.Context, key string) (ok bool, err errors.CodeError) {
	proxy, proxyErr := ctx.App().ServiceProxy(ctx, Namespace)
	if proxyErr != nil {
		err = errors.New(555, "***WARNING***", fmt.Sprintf("fns Redis Proxy: get %s proxy failed", Namespace)).WithCause(proxyErr)
		return
	}
	arg, argErr := fns.NewArgument(key)
	if argErr != nil {
		err = argErr
		return
	}

	r := proxy.Request(ctx, PersistFn, arg)
	err = r.Get(ctx, &ok)
	return
}

func TTL(ctx fns.Context, key string) (ttl time.Duration, err errors.CodeError) {
	proxy, proxyErr := ctx.App().ServiceProxy(ctx, Namespace)
	if proxyErr != nil {
		err = errors.New(555, "***WARNING***", fmt.Sprintf("fns Redis Proxy: get %s proxy failed", Namespace)).WithCause(proxyErr)
		return
	}
	arg, argErr := fns.NewArgument(key)
	if argErr != nil {
		err = argErr
		return
	}

	r := proxy.Request(ctx, TTLFn, arg)
	err = r.Get(ctx, &ttl)
	return
}

func Set(ctx fns.Context, param SetParam) (err errors.CodeError) {
	proxy, proxyErr := ctx.App().ServiceProxy(ctx, Namespace)
	if proxyErr != nil {
		err = errors.New(555, "***WARNING***", fmt.Sprintf("fns Redis Proxy: get %s proxy failed", Namespace)).WithCause(proxyErr)
		return
	}
	arg, argErr := fns.NewArgument(param)
	if argErr != nil {
		err = argErr
		return
	}

	r := proxy.Request(ctx, SetFn, arg)
	err = r.Get(ctx, &json.RawMessage{})
	return
}

func Get(ctx fns.Context, key string) (result json.RawMessage, err errors.CodeError) {
	proxy, proxyErr := ctx.App().ServiceProxy(ctx, Namespace)
	if proxyErr != nil {
		err = errors.New(555, "***WARNING***", fmt.Sprintf("fns Redis Proxy: get %s proxy failed", Namespace)).WithCause(proxyErr)
		return
	}
	arg, argErr := fns.NewArgument(key)
	if argErr != nil {
		err = argErr
		return
	}
	result = json.RawMessage{}
	r := proxy.Request(ctx, GetFn, arg)
	err = r.Get(ctx, &result)
	return
}

func Incr(ctx fns.Context, key string) (err errors.CodeError) {
	proxy, proxyErr := ctx.App().ServiceProxy(ctx, Namespace)
	if proxyErr != nil {
		err = errors.New(555, "***WARNING***", fmt.Sprintf("fns Redis Proxy: get %s proxy failed", Namespace)).WithCause(proxyErr)
		return
	}
	arg, argErr := fns.NewArgument(key)
	if argErr != nil {
		err = argErr
		return
	}
	r := proxy.Request(ctx, IncrFn, arg)
	err = r.Get(ctx, &json.RawMessage{})
	return
}

func Decr(ctx fns.Context, key string) (err errors.CodeError) {
	proxy, proxyErr := ctx.App().ServiceProxy(ctx, Namespace)
	if proxyErr != nil {
		err = errors.New(555, "***WARNING***", fmt.Sprintf("fns Redis Proxy: get %s proxy failed", Namespace)).WithCause(proxyErr)
		return
	}
	arg, argErr := fns.NewArgument(key)
	if argErr != nil {
		err = argErr
		return
	}
	r := proxy.Request(ctx, DecrFn, arg)
	err = r.Get(ctx, &json.RawMessage{})
	return
}

func Lock(ctx fns.Context, param LockParam) (err errors.CodeError) {
	proxy, proxyErr := ctx.App().ServiceProxy(ctx, Namespace)
	if proxyErr != nil {
		err = errors.New(555, "***WARNING***", fmt.Sprintf("fns Redis Proxy: get %s proxy failed", Namespace)).WithCause(proxyErr)
		return
	}
	arg, argErr := fns.NewArgument(param)
	if argErr != nil {
		err = argErr
		return
	}
	r := proxy.Request(ctx, LockFn, arg)
	err = r.Get(ctx, &json.RawMessage{})
	return
}

func Unlock(ctx fns.Context, key string) (err errors.CodeError) {
	proxy, proxyErr := ctx.App().ServiceProxy(ctx, Namespace)
	if proxyErr != nil {
		err = errors.New(555, "***WARNING***", fmt.Sprintf("fns Redis Proxy: get %s proxy failed", Namespace)).WithCause(proxyErr)
		return
	}
	arg, argErr := fns.NewArgument(key)
	if argErr != nil {
		err = argErr
		return
	}
	r := proxy.Request(ctx, UnlockFn, arg)
	err = r.Get(ctx, &json.RawMessage{})
	return
}

func ZAdd(ctx fns.Context, param ZAddParam) (err errors.CodeError) {
	proxy, proxyErr := ctx.App().ServiceProxy(ctx, Namespace)
	if proxyErr != nil {
		err = errors.New(555, "***WARNING***", fmt.Sprintf("fns Redis Proxy: get %s proxy failed", Namespace)).WithCause(proxyErr)
		return
	}
	arg, argErr := fns.NewArgument(param)
	if argErr != nil {
		err = argErr
		return
	}
	r := proxy.Request(ctx, ZAddFn, arg)
	err = r.Get(ctx, &json.RawMessage{})
	return
}

func ZCard(ctx fns.Context, key string) (num int64, err errors.CodeError) {
	proxy, proxyErr := ctx.App().ServiceProxy(ctx, Namespace)
	if proxyErr != nil {
		err = errors.New(555, "***WARNING***", fmt.Sprintf("fns Redis Proxy: get %s proxy failed", Namespace)).WithCause(proxyErr)
		return
	}
	arg, argErr := fns.NewArgument(key)
	if argErr != nil {
		err = argErr
		return
	}
	r := proxy.Request(ctx, ZCardFn, arg)
	err = r.Get(ctx, &num)
	return
}

func ZRange(ctx fns.Context, param ZRangeParam) (result *json.Array, err errors.CodeError) {
	proxy, proxyErr := ctx.App().ServiceProxy(ctx, Namespace)
	if proxyErr != nil {
		err = errors.New(555, "***WARNING***", fmt.Sprintf("fns Redis Proxy: get %s proxy failed", Namespace)).WithCause(proxyErr)
		return
	}
	arg, argErr := fns.NewArgument(param)
	if argErr != nil {
		err = argErr
		return
	}
	result = json.NewArray()
	r := proxy.Request(ctx, ZRangeFn, arg)
	err = r.Get(ctx, &result)
	return
}

func zRangeByScore(ctx fns.Context, param ZRangeByScoreParam) (result *json.Array, err errors.CodeError) {
	proxy, proxyErr := ctx.App().ServiceProxy(ctx, Namespace)
	if proxyErr != nil {
		err = errors.New(555, "***WARNING***", fmt.Sprintf("fns Redis Proxy: get %s proxy failed", Namespace)).WithCause(proxyErr)
		return
	}
	arg, argErr := fns.NewArgument(param)
	if argErr != nil {
		err = argErr
		return
	}
	result = json.NewArray()
	r := proxy.Request(ctx, ZRangeByScoreFn, arg)
	err = r.Get(ctx, &result)
	return
}

func ZRem(ctx fns.Context, param ZRemParam) (ok bool, err errors.CodeError) {
	proxy, proxyErr := ctx.App().ServiceProxy(ctx, Namespace)
	if proxyErr != nil {
		err = errors.New(555, "***WARNING***", fmt.Sprintf("fns Redis Proxy: get %s proxy failed", Namespace)).WithCause(proxyErr)
		return
	}
	arg, argErr := fns.NewArgument(param)
	if argErr != nil {
		err = argErr
		return
	}
	r := proxy.Request(ctx, ZRemFn, arg)
	err = r.Get(ctx, &ok)
	return
}

func ZRemByRank(ctx fns.Context, param ZRemByRankParam) (ok bool, err errors.CodeError) {
	proxy, proxyErr := ctx.App().ServiceProxy(ctx, Namespace)
	if proxyErr != nil {
		err = errors.New(555, "***WARNING***", fmt.Sprintf("fns Redis Proxy: get %s proxy failed", Namespace)).WithCause(proxyErr)
		return
	}
	arg, argErr := fns.NewArgument(param)
	if argErr != nil {
		err = argErr
		return
	}
	r := proxy.Request(ctx, ZRemByRankFn, arg)
	err = r.Get(ctx, &ok)
	return
}

func ZRemByScore(ctx fns.Context, param ZRemByScoreParam) (ok bool, err errors.CodeError) {
	proxy, proxyErr := ctx.App().ServiceProxy(ctx, Namespace)
	if proxyErr != nil {
		err = errors.New(555, "***WARNING***", fmt.Sprintf("fns Redis Proxy: get %s proxy failed", Namespace)).WithCause(proxyErr)
		return
	}
	arg, argErr := fns.NewArgument(param)
	if argErr != nil {
		err = argErr
		return
	}
	r := proxy.Request(ctx, ZRemByScoreFn, arg)
	err = r.Get(ctx, &ok)
	return
}

func ZRevRange(ctx fns.Context, param ZRevRangeParam) (result *json.Array, err errors.CodeError) {
	proxy, proxyErr := ctx.App().ServiceProxy(ctx, Namespace)
	if proxyErr != nil {
		err = errors.New(555, "***WARNING***", fmt.Sprintf("fns Redis Proxy: get %s proxy failed", Namespace)).WithCause(proxyErr)
		return
	}
	arg, argErr := fns.NewArgument(param)
	if argErr != nil {
		err = argErr
		return
	}
	result = json.NewArray()
	r := proxy.Request(ctx, ZRevRangeFn, arg)
	err = r.Get(ctx, &result)
	return
}

func ZRevRangeByScore(ctx fns.Context, param ZRevRangeByScoreParam) (result *json.Array, err errors.CodeError) {
	proxy, proxyErr := ctx.App().ServiceProxy(ctx, Namespace)
	if proxyErr != nil {
		err = errors.New(555, "***WARNING***", fmt.Sprintf("fns Redis Proxy: get %s proxy failed", Namespace)).WithCause(proxyErr)
		return
	}
	arg, argErr := fns.NewArgument(param)
	if argErr != nil {
		err = argErr
		return
	}
	result = json.NewArray()
	r := proxy.Request(ctx, ZRevRangeByScoreFn, arg)
	err = r.Get(ctx, &result)
	return
}
