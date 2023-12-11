package redis

import (
	"github.com/aacfactory/errors"
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
	commands := make([]Command, 0, 1)
	paramErr := ctx.Param().TransformTo(&commands)
	if paramErr != nil {
		err = errors.Warning("redis: parse param failed").WithCause(paramErr)
		return
	}
	commandsLen := len(commands)
	switch commandsLen {
	case 0:
		err = errors.Warning("redis: invalid param")
		break
	case 1:
		resp := handler.client.Do(ctx, commands[0].as(handler.client))
		results := make([]Result, commandsLen)
		results[0] = newResult(resp)
		v = results
		break
	default:
		completes := make([]rueidis.Completed, commandsLen)
		for i, command := range commands {
			completes[i] = command.as(handler.client)
		}
		resp := handler.client.DoMulti(ctx, completes...)
		results := make([]Result, commandsLen)
		for i, redisResult := range resp {
			results[i] = newResult(redisResult)
		}
		v = results
		break
	}
	return
}
