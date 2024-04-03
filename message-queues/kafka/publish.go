package kafka

import (
	"fmt"
	"github.com/aacfactory/errors"
	"github.com/aacfactory/fns/context"
	"github.com/aacfactory/fns/runtime"
	"github.com/aacfactory/fns/services"
)

var (
	publishFnName = []byte("publish")
)

type ProducerMessage struct {
	Topic   string  `json:"topic" avro:"topic"`
	Key     []byte  `json:"key" avro:"key"`
	Body    []byte  `json:"body" avro:"body"`
	Headers Headers `json:"headers" avro:"headers"`
}

func (msg ProducerMessage) AddHeader(key string, value []byte) ProducerMessage {
	msg.Headers = append(msg.Headers, Header{
		Key:   key,
		Value: value,
	})
	return msg
}

func (msg ProducerMessage) Validate() (err error) {
	if msg.Topic == "" || len(msg.Key) == 0 || len(msg.Body) == 0 {
		err = errors.Warning("kafka: invalid message")
	}
	return
}

func NewMessage(topic string, key []byte, body []byte) ProducerMessage {
	return ProducerMessage{
		Topic:   topic,
		Key:     key,
		Body:    body,
		Headers: nil,
	}
}

func Publish(ctx context.Context, message ...ProducerMessage) (err error) {
	if len(message) == 0 {
		err = errors.Warning("kafka: publish failed").WithCause(fmt.Errorf("message is required"))
		return
	}
	for _, msg := range message {
		err = msg.Validate()
		if err != nil {
			return
		}
	}
	_, err = runtime.Endpoints(ctx).Request(ctx, endpointName, publishFnName, publishParam{
		Async:    false,
		Messages: message,
	})
	return
}

func PublishAsync(ctx context.Context, message ...ProducerMessage) (err error) {
	if len(message) == 0 {
		err = errors.Warning("kafka: publish async failed").WithCause(fmt.Errorf("message is required"))
		return
	}
	for _, msg := range message {
		err = msg.Validate()
		if err != nil {
			return
		}
	}
	_, err = runtime.Endpoints(ctx).Request(ctx, endpointName, publishFnName, publishParam{
		Async:    true,
		Messages: message,
	})
	return
}

type publishParam struct {
	Messages []ProducerMessage `json:"messages" avro:"messages"`
	Async    bool              `json:"async" avro:"async"`
}

type publishFn struct {
	producer *Producer
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
	if fn.producer == nil {
		err = errors.Warning("kafka: publish failed").WithCause(fmt.Errorf("no producer"))
		return
	}
	param, paramErr := services.ValueOfParam[publishParam](ctx.Param())
	if paramErr != nil {
		err = errors.Warning("kafka: publish failed").WithCause(paramErr)
		return
	}
	err = fn.producer.Publish(ctx, param.Messages, param.Async)
	if err != nil {
		err = errors.Warning("kafka: publish failed").WithCause(err)
		return
	}
	return
}
