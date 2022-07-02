package kafka

import (
	"context"
	"fmt"
	"github.com/aacfactory/configures"
	"github.com/aacfactory/errors"
	"github.com/aacfactory/logs"
	"github.com/segmentio/kafka-go"
	"strings"
)

func newConsumer(topic string, log logs.Logger, config *ConsumerConfig, brokers []string) (consumer *Consumer, err error) {
	log = log.With("consumer", topic)
	topic = strings.TrimSpace(topic)
	if topic == "" {
		err = errors.Warning(fmt.Sprintf("kafka: new %s consumer failed", topic)).WithCause(fmt.Errorf("topic is required"))
		return
	}
	groupId := strings.TrimSpace(config.GroupId)
	if groupId == "" {
		err = errors.Warning(fmt.Sprintf("kafka: new %s consumer failed", topic)).WithCause(fmt.Errorf("group id is required"))
		return
	}
	handlerName := strings.TrimSpace(config.Handler)
	if handlerName == "" {
		handlerName = "default"
	}
	handlerBuilder, hasHandler := consumerHandlers[handlerName]
	if !hasHandler {
		err = errors.Warning(fmt.Sprintf("kafka: new %s consumer failed", topic)).WithCause(fmt.Errorf("%s handler is not registered", handlerName))
		return
	}
	var handlerConfig configures.Config
	if config.HandlerOptions != nil && len(config.HandlerOptions) > 2 {
		handlerConfig, err = configures.NewJsonConfig(config.HandlerOptions)
		if err != nil {
			err = errors.Warning(fmt.Sprintf("kafka: new %s consumer failed", topic)).WithCause(fmt.Errorf("%s handler options is invalied", handlerName)).WithCause(err)
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
		err = errors.Warning(fmt.Sprintf("kafka: new %s consumer failed", topic)).WithCause(fmt.Errorf("%s handler build failed", handlerName)).WithCause(handlerErr)
		return
	}
	kafkaReader := kafka.NewReader(kafka.ReaderConfig{
		Brokers:  brokers,
		GroupID:  groupId,
		Topic:    topic,
		MinBytes: 4e3,
		MaxBytes: 4e6,
		Logger: &Printf{
			Core: log,
		},
		ErrorLogger: &Printf{
			Core: log,
		},
		IsolationLevel: kafka.ReadCommitted,
	})

	var reader MessageReader
	var committer MessageCommitter
	if !config.AutoCommit {
		committer = &ExplicitMessageCommitter{
			reader: kafkaReader,
		}
		reader = &MessageFetcher{
			kafkaReader,
		}
	} else {
		committer = &NoopMessageCommitter{}
		reader = kafkaReader
	}
	consumer = &Consumer{
		log:       log,
		reader:    reader,
		topic:     topic,
		groupId:   groupId,
		cancel:    nil,
		committer: committer,
		handler:   handler,
	}
	return
}

type Consumer struct {
	log       logs.Logger
	topic     string
	groupId   string
	reader    MessageReader
	committer MessageCommitter
	cancel    func()
	handler   ConsumerHandler
}

func (consumer *Consumer) Consume(ctx context.Context) (err error) {
	ctx, consumer.cancel = context.WithCancel(ctx)
	ctx = context.WithValue(ctx, committerContextKey, consumer.committer)
	for {
		stopped := false
		select {
		case <-ctx.Done():
			stopped = true
			break
		default:
			consumer.read(ctx)
		}
		if stopped {
			break
		}
	}
	return
}

func (consumer *Consumer) read(ctx context.Context) {
	msg, readErr := consumer.reader.ReadMessage(ctx)
	if readErr != nil {
		if consumer.log.WarnEnabled() {
			consumer.log.Warn().Cause(readErr).Message(fmt.Sprintf("kafka: consume message failed"))
		}
		return
	}
	consumer.handler.Handle(ctx, &DefaultConsumerMessage{
		raw: msg,
	})
	return
}

func (consumer *Consumer) Close() (err error) {
	consumer.cancel()
	_ = consumer.reader.Close()
	return
}

type MessageReader interface {
	ReadMessage(ctx context.Context) (msg kafka.Message, err error)
	Close() (err error)
}

type MessageFetcher struct {
	reader *kafka.Reader
}

func (fetcher *MessageFetcher) ReadMessage(ctx context.Context) (msg kafka.Message, err error) {
	msg, err = fetcher.reader.FetchMessage(ctx)
	return
}

func (fetcher *MessageFetcher) Close() (err error) {
	err = fetcher.reader.Close()
	return
}
