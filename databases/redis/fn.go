package redis

import (
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

	return
}
