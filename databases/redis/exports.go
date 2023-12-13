package redis

import (
	"fmt"
	"github.com/aacfactory/errors"
	"github.com/aacfactory/fns/commons/bytex"
	"github.com/aacfactory/fns/context"
	"github.com/aacfactory/fns/runtime"
	"github.com/aacfactory/fns/services"
	"github.com/redis/rueidis"
)

type exportsFn struct {
	id     string
	client rueidis.Client
}

func (handler *exportsFn) Name() string {
	return string(exportsFnName)
}

func (handler *exportsFn) Internal() bool {
	return true
}

func (handler *exportsFn) Readonly() bool {
	return false
}

func (handler *exportsFn) Handle(ctx services.Request) (v any, err error) {
	param, paramErr := services.ValueOfParam[ExportParam](ctx.Param())
	if paramErr != nil {
		err = errors.Warning("redis: export failed").WithCause(paramErr)
		return
	}
	if param.Id == handler.id {
		v = handler.client
		return
	}
	err = errors.Warning("redis: export failed").WithCause(fmt.Errorf("not in same app"))
	return
}

type ExportParam struct {
	Id string `json:"id"`
}

func Export(ctx context.Context) (client rueidis.Client, err error) {
	ep := used(ctx)
	if len(ep) == 0 {
		ep = endpointName
	}
	id := runtime.AppId(ctx)
	eps := runtime.Endpoints(ctx)
	response, handleErr := eps.Request(ctx, ep, exportsFnName, ExportParam{
		Id: bytex.ToString(id),
	})
	if handleErr != nil {
		err = handleErr
		return
	}

	client, err = services.ValueOfResponse[rueidis.Client](response)
	if err != nil {
		err = errors.Warning("redis: export failed").WithCause(err)
		return
	}
	return
}
