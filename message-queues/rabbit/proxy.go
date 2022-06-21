package rabbit

import (
	"context"
	"github.com/aacfactory/errors"
	"github.com/aacfactory/fns/service"
	"github.com/aacfactory/json"
)

type PublishArgument struct {
	Name string          `json:"name"`
	Body json.RawMessage `json:"body"`
}

type PublishResult struct {
	Succeed bool `json:"succeed"`
}

func Publish(ctx context.Context, argument PublishArgument) (ok bool, err errors.CodeError) {
	endpoint, hasEndpoint := service.GetEndpoint(ctx, name)
	if !hasEndpoint {
		err = errors.NotFound("rabbitmq: endpoint was not found")
		return
	}
	fr := endpoint.Request(ctx, "publish", service.NewArgument(argument))
	result := PublishResult{}
	_, getResultErr := fr.Get(ctx, &result)
	if getResultErr != nil {
		err = getResultErr
		return
	}
	ok = result.Succeed
	return
}
