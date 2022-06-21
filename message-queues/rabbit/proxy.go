package rabbit

import (
	"context"
	"github.com/aacfactory/errors"
	"github.com/aacfactory/fns/service"
	"github.com/aacfactory/json"
)

type ProduceArgument struct {
	Name string          `json:"name"`
	Body json.RawMessage `json:"body"`
}

type ProduceResult struct {
	Succeed bool `json:"succeed"`
}

func Produce(ctx context.Context, argument ProduceArgument) (ok bool, err errors.CodeError) {
	endpoint, hasEndpoint := service.GetEndpoint(ctx, name)
	if !hasEndpoint {
		err = errors.NotFound("rabbitmq: endpoint was not found")
		return
	}
	fr := endpoint.Request(ctx, "produce", service.NewArgument(argument))
	result := ProduceResult{}
	_, getResultErr := fr.Get(ctx, &result)
	if getResultErr != nil {
		err = getResultErr
		return
	}
	ok = result.Succeed
	return
}
