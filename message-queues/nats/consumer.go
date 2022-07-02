package nats

import (
	"context"
	"fmt"
	"github.com/aacfactory/configures"
	"github.com/aacfactory/errors"
	"github.com/aacfactory/logs"
	"github.com/nats-io/nats.go"
	"strings"
)

func newConsumer(conn *nats.Conn, name string, log logs.Logger, config *ConsumerConfig) (consumer *Consumer, err error) {
	subject := strings.TrimSpace(config.Subject)
	if subject == "" {
		err = errors.Warning(fmt.Sprintf("nats: new %s consumer failed", name)).WithCause(fmt.Errorf("subject is required"))
		return
	}
	queue := strings.TrimSpace(config.Queue)
	handlerName := strings.TrimSpace(config.Handler)
	if handlerName == "" {
		handlerName = "default"
	}
	handlerBuilder, hasHandler := consumerHandlers[handlerName]
	if !hasHandler {
		err = errors.Warning(fmt.Sprintf("nats: new %s consumer failed", name)).WithCause(fmt.Errorf("%s handler is not registered", handlerName))
		return
	}
	var handlerConfig configures.Config
	if config.HandlerOptions != nil && len(config.HandlerOptions) > 2 {
		handlerConfig, err = configures.NewJsonConfig(config.HandlerOptions)
		if err != nil {
			err = errors.Warning(fmt.Sprintf("nats: new %s consumer failed", name)).WithCause(fmt.Errorf("%s handler options is invalied", handlerName)).WithCause(err)
			return
		}
	} else {
		handlerConfig, _ = configures.NewJsonConfig([]byte{'{', '}'})
	}
	handler, handlerErr := handlerBuilder(ConsumerHandlerOptions{
		Log:    log.With("handler", handlerName),
		Config: handlerConfig,
	})
	if handlerErr != nil {
		err = errors.Warning(fmt.Sprintf("nats: new %s consumer failed", name)).WithCause(fmt.Errorf("%s handler build failed", handlerName)).WithCause(handlerErr)
		return
	}
	consumer = &Consumer{
		name:    name,
		conn:    conn,
		subject: subject,
		queue:   queue,
		ctx:     nil,
		sub:     nil,
		handler: handler,
	}
	return
}

type Consumer struct {
	name    string
	conn    *nats.Conn
	subject string
	queue   string
	ctx     context.Context
	cancel  func()
	sub     *nats.Subscription
	handler ConsumerHandler
}

func (consumer *Consumer) Consume(ctx context.Context) (err error) {
	consumer.ctx, consumer.cancel = context.WithCancel(ctx)
	if consumer.queue == "" {
		sub, subErr := consumer.conn.Subscribe(consumer.subject, consumer.msgHandle)
		if subErr != nil {
			err = errors.Warning(fmt.Sprintf("nats: %s consume failed", consumer.name)).WithCause(subErr)
			return
		}
		consumer.sub = sub
	} else {
		sub, subErr := consumer.conn.QueueSubscribe(consumer.subject, consumer.queue, consumer.msgHandle)
		if subErr != nil {
			err = errors.Warning(fmt.Sprintf("nats: %s consume failed", consumer.name)).WithCause(subErr)
			return
		}
		consumer.sub = sub
	}
	<-ctx.Done()
	return
}

func (consumer *Consumer) msgHandle(msg *nats.Msg) {
	consumer.handler.Handle(consumer.ctx, &DefaultConsumerMessage{
		raw: msg,
	})
}

func (consumer *Consumer) Close() (err error) {
	consumer.cancel()
	_ = consumer.sub.Drain()
	return
}
