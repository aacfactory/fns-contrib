package redis

import (
	"context"
	"fmt"
	"github.com/aacfactory/errors"
	"github.com/aacfactory/fns-contrib/databases/redis/internal"
	"github.com/aacfactory/fns/service"
	"github.com/aacfactory/logs"
)

const (
	name      = "redis"
	commandFn = "command"
)

func Service() service.Service {
	return &_service_{}
}

type _service_ struct {
	log      logs.Logger
	database internal.Database
}

func (svc *_service_) Name() string {
	return name
}

func (svc *_service_) Internal() bool {
	return true
}

func (svc *_service_) Build(options service.Options) (err error) {

	return
}

func (svc *_service_) Components() (components map[string]service.Component) {
	return
}
func (svc *_service_) Document() (doc service.Document) {
	return
}

func (svc *_service_) Handle(context context.Context, fn string, argument service.Argument) (result interface{}, err errors.CodeError) {
	switch fn {
	case commandFn:
		command := &Command{}
		argumentErr := argument.As(command)
		if argumentErr != nil {
			err = errors.BadRequest("redis: invalid command argument").WithCause(argumentErr).WithMeta("service", name).WithMeta("fn", fn)
			return
		}
		commandName := command.Name
		if commandName == "" {
			err = errors.BadRequest("redis: invalid command argument").WithCause(fmt.Errorf("command name is empty")).WithMeta("service", name).WithMeta("fn", fn)
			return
		}
		commandParams, paramsErr := command.Params.convert()
		if paramsErr != nil {
			err = errors.BadRequest("redis: invalid command argument").WithCause(paramsErr).WithMeta("service", name).WithMeta("fn", fn)
			return
		}
		handleResult, handleErr := svc.database.HandleCommand(context, commandName, commandParams)
		if handleErr != nil {
			if handleErr.Code() == 404 {
				result = &Result{
					Exist: false,
					Value: nil,
				}
				return
			}
			err = handleErr.WithMeta("service", name).WithMeta("fn", fn)
			return
		}
		result = &Result{
			Exist: true,
			Value: handleResult,
		}
		break
	default:
		err = errors.NotFound("redis: fn was not found").WithMeta("service", name).WithMeta("fn", fn)
		break
	}
	return
}

func (svc *_service_) Close() {
	svc.database.Close()
	return
}
