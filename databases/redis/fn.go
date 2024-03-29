package redis

import (
	"github.com/aacfactory/errors"
	"github.com/aacfactory/fns/commons/bytex"
	"github.com/aacfactory/fns/context"
	"github.com/aacfactory/fns/runtime"
	"github.com/aacfactory/fns/services"
	"github.com/redis/rueidis"
)

type commandHandler struct {
	client rueidis.Client
}

func (handler *commandHandler) Name() string {
	return string(commandFnName)
}

func (handler *commandHandler) Internal() bool {
	return true
}

func (handler *commandHandler) Readonly() bool {
	return false
}

func (handler *commandHandler) Handle(ctx services.Request) (v any, err error) {
	commands := make(Commands, 0)
	paramErr := ctx.Param().Unmarshal(&commands)
	if paramErr != nil {
		err = errors.Warning("redis: parse param failed").WithCause(paramErr)
		return
	}
	var resp []rueidis.RedisResult
	commandsLen := commands.Len()
	switch commandsLen {
	case 0:
		err = errors.Warning("redis: invalid param")
		return
	case 1:
		cc, ok := commands.Build(handler.client)
		if !ok {
			err = errors.Warning("redis: invalid param")
			return
		}
		resp = append(resp, handler.client.Do(ctx, cc[0]))
		break
	default:
		cc, ok := commands.Build(handler.client)
		if !ok {
			err = errors.Warning("redis: invalid param")
			return
		}
		resp = handler.client.DoMulti(ctx, cc...)
		break
	}
	results := make([]result, commandsLen)
	for i, redisResult := range resp {
		results[i] = newResult(redisResult)
	}
	v = results
	return
}

func Do(ctx context.Context, command IncompleteCommand) (v Result, err error) {
	vv, doErr := DoMulti(ctx, command)
	if doErr != nil {
		err = doErr
		return
	}
	r, has := vv.Next()
	if has {
		v = r
	}
	return
}

func DoMulti(ctx context.Context, commands ...IncompleteCommand) (v Results, err error) {
	ep := used(ctx)
	if len(ep) == 0 {
		ep = endpointName
	}
	commandsLen := len(commands)
	if commandsLen == 0 {
		err = errors.Warning("redis: invalid commands").WithMeta("endpoint", bytex.ToString(ep))
		return
	}
	cacheables := make(Commands, len(commands))
	for i, command := range commands {
		cacheables[i] = command.Build()
	}
	eps := runtime.Endpoints(ctx)
	response, handleErr := eps.Request(ctx, ep, commandFnName, cacheables)
	if handleErr != nil {
		err = handleErr
		return
	}
	r, rErr := services.ValueOfResponse[[]result](response)
	if rErr != nil {
		err = rErr
		return
	}
	v = newResults(r)
	return
}
