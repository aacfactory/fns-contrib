package kafka

import (
	"fmt"
	"github.com/aacfactory/errors"
	"github.com/aacfactory/fns/context"
	"github.com/aacfactory/fns/runtime"
	"github.com/aacfactory/fns/services"
	"time"
)

var (
	publishFnName = []byte("publish")
)

func NewMessage(key []byte, body []byte) WriteMessage {
	return WriteMessage{
		Key:     key,
		Body:    body,
		Headers: nil,
		Offset:  0,
		Time:    time.Time{},
	}
}

func Publish(ctx context.Context, topic string, message ...WriteMessage) (err error) {
	if topic == "" {
		err = errors.Warning("kafka: publish failed").WithCause(fmt.Errorf("topic is required"))
		return
	}
	if len(message) == 0 {
		err = errors.Warning("kafka: publish failed").WithCause(fmt.Errorf("message is required"))
		return
	}
	_, err = runtime.Endpoints(ctx).Request(ctx, endpointName, publishFnName, publishParam{
		Topic:    topic,
		Messages: message,
	})
	return
}

type publishParam struct {
	Topic    string         `json:"topic"`
	Messages []WriteMessage `json:"messages"`
}

type publishFn struct {
	writers map[string]*Writer
}

func (fn *publishFn) Name() string {
	return string(publishFnName)
}

func (fn *publishFn) Internal() bool {
	return true
}

func (fn *publishFn) Readonly() bool {
	return false
}

func (fn *publishFn) Handle(ctx services.Request) (v any, err error) {
	param, paramErr := services.ValueOfParam[publishParam](ctx.Param())
	if paramErr != nil {
		err = errors.Warning("kafka: publish failed").WithCause(paramErr)
		return
	}
	if param.Topic == "" || len(param.Messages) == 0 {
		return
	}
	w, has := fn.writers[param.Topic]
	if !has {
		err = errors.Warning("kafka: publish failed").WithCause(fmt.Errorf("there is no %s topic writer", param.Topic))
		return
	}
	err = w.Write(ctx, param.Messages)
	if err != nil {
		err = errors.Warning("kafka: publish failed").WithCause(err)
		return
	}
	return
}
