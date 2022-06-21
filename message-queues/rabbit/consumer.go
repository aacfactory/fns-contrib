package rabbit

import (
	"context"
	"fmt"
	"github.com/aacfactory/configuares"
	"github.com/aacfactory/errors"
	"github.com/aacfactory/logs"
	amqp "github.com/rabbitmq/amqp091-go"
	"strings"
)

func newConsumer(conn *amqp.Connection, name string, log logs.Logger, config *ConsumerConfig) (consumer *Consumer, err error) {
	queue := strings.TrimSpace(config.Queue)
	if queue == "" {
		err = errors.Warning(fmt.Sprintf("rabbitmq: new %s consumer failed", name)).WithCause(fmt.Errorf("queue is required"))
		return
	}
	handlerName := strings.TrimSpace(config.Handler)
	if handlerName == "" {
		handlerName = "default"
	}
	handlerBuilder, hasHandler := consumerHandlers[handlerName]
	if !hasHandler {
		err = errors.Warning(fmt.Sprintf("rabbitmq: new %s consumer failed", name)).WithCause(fmt.Errorf("%s handler is not registered", handlerName))
		return
	}
	var handlerConfig configuares.Config
	if config.HandlerOptions != nil && len(config.HandlerOptions) > 2 {
		handlerConfig, err = configuares.NewJsonConfig(config.HandlerOptions)
		if err != nil {
			err = errors.Warning(fmt.Sprintf("rabbitmq: new %s consumer failed", name)).WithCause(fmt.Errorf("%s handler options is invalied", handlerName)).WithCause(err)
			return
		}
	} else {
		handlerConfig, _ = configuares.NewJsonConfig([]byte{'{', '}'})
	}
	handler, handlerErr := handlerBuilder(ConsumerHandlerOptions{
		Log:    log.With("handler", handlerName),
		Config: handlerConfig,
	})
	if handlerErr != nil {
		err = errors.Warning(fmt.Sprintf("rabbitmq: new %s consumer failed", name)).WithCause(fmt.Errorf("%s handler build failed", handlerName)).WithCause(handlerErr)
		return
	}
	consumer = &Consumer{
		name:      name,
		conn:      conn,
		channel:   nil,
		queue:     queue,
		autoAck:   config.AutoAck,
		exclusive: config.Exclusive,
		noLocal:   config.NoLocal,
		noWait:    config.NoWait,
		arguments: config.Arguments,
		handler:   handler,
	}
	return
}

type Consumer struct {
	name      string
	conn      *amqp.Connection
	channel   *amqp.Channel
	queue     string
	autoAck   bool
	exclusive bool
	noLocal   bool
	noWait    bool
	arguments map[string]interface{}
	handler   ConsumerHandler
}

func (consumer *Consumer) Consume(ctx context.Context) (err error) {
	ch, chErr := consumer.conn.Channel()
	if chErr != nil {
		err = errors.Warning(fmt.Sprintf("rabbitmq: %s consume failed", consumer.name)).WithCause(chErr)
		return
	}
	consumer.channel = ch
	delivery, consumeErr := ch.Consume(consumer.queue, consumer.name, consumer.autoAck, consumer.noLocal, consumer.exclusive, consumer.noWait, consumer.arguments)
	if consumeErr != nil {
		err = errors.Warning(fmt.Sprintf("rabbitmq: %s consume failed", consumer.name)).WithCause(consumeErr)
		return
	}
	for {
		raw, ok := <-delivery
		if !ok {
			break
		}
		msg := &DefaultConsumerMessage{
			raw: &raw,
		}
		consumer.handler.Handle(ctx, msg)
	}
	return
}

func (consumer *Consumer) Close() (err error) {
	_ = consumer.channel.Close()
	return
}
