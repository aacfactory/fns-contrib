package redis

import (
	"github.com/aacfactory/errors"
	"github.com/aacfactory/fns/commons/bytex"
	"github.com/aacfactory/fns/context"
	"github.com/aacfactory/fns/runtime"
	"github.com/aacfactory/fns/services"
	"github.com/redis/rueidis"
)

type cacheableHandler struct {
	client       rueidis.Client
	disableCache bool
	handler      *commandHandler
}

func (handler *cacheableHandler) Name() string {
	return string(cacheableFnName)
}

func (handler *cacheableHandler) Internal() bool {
	return true
}

func (handler *cacheableHandler) Readonly() bool {
	return false
}

func (handler *cacheableHandler) Handle(ctx services.Request) (v any, err error) {
	if handler.disableCache {
		v, err = handler.handler.Handle(ctx)
		return
	}
	commands := make(Cacheables, 0)
	paramErr := ctx.Param().Unmarshal(&commands)
	if paramErr != nil {
		err = errors.Warning("redis: parse param failed").WithCause(paramErr)
		return
	}
	if !commands.Valid() {
		err = errors.Warning("redis: invalid param")
		return
	}
	var resp []rueidis.RedisResult
	commandsLen := commands.Len()
	switch commandsLen {
	case 0:
		err = errors.Warning("redis: invalid param")
		return
	case 1:
		cacheables, ok := commands.Build(handler.client)
		if !ok {
			err = errors.Warning("redis: invalid param")
			return
		}
		resp = append(resp, handler.client.DoCache(ctx, cacheables[0].Cmd, cacheables[0].TTL))
		break
	default:
		cacheables, ok := commands.Build(handler.client)
		if !ok {
			err = errors.Warning("redis: invalid param")
			return
		}
		resp = handler.client.DoMultiCache(ctx, cacheables...)
		break
	}
	results := make([]result, commandsLen)
	for i, redisResult := range resp {
		results[i] = newResult(redisResult)
	}
	v = results
	return
}

func DoCache(ctx context.Context, command IncompleteCommand) (v Result, err error) {
	vv, doErr := DoMultiCache(ctx, command)
	if doErr != nil {
		err = doErr
		return
	}
	v = vv[0]
	return
}

func DoMultiCache(ctx context.Context, commands ...IncompleteCommand) (v []Result, err error) {
	ep := used(ctx)
	if len(ep) == 0 {
		ep = endpointName
	}
	commandsLen := len(commands)
	if commandsLen == 0 {
		err = errors.Warning("redis: invalid commands").WithMeta("endpoint", bytex.ToString(ep))
		return
	}
	cacheables := make(Cacheables, len(commands))
	for i, command := range commands {
		cacheables[i] = command.Build()
	}
	if !cacheables.Valid() {
		err = errors.Warning("redis: invalid commands").WithMeta("endpoint", bytex.ToString(ep))
		return
	}
	eps := runtime.Endpoints(ctx)
	response, handleErr := eps.Request(ctx, ep, cacheableFnName, cacheables)
	if handleErr != nil {
		err = handleErr
		return
	}
	r := make([]result, 0, 1)
	err = response.Unmarshal(&r)
	if err != nil {
		return
	}
	v = make([]Result, len(r))
	for i, e := range r {
		v[i] = e
	}
	return
}
