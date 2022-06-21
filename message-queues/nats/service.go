package nats

import (
	"context"
	"fmt"
	"github.com/aacfactory/errors"
	"github.com/aacfactory/fns/service"
	"github.com/aacfactory/logs"
	"github.com/nats-io/nats.go"
	"strings"
	"time"
)

const (
	_name = "nats"
)

func Service() service.Service {
	return &_service_{
		log:       nil,
		conn:      nil,
		producers: make(map[string]*Producer),
		consumers: make(map[string]*Consumer),
	}
}

type _service_ struct {
	log       logs.Logger
	conn      *nats.Conn
	producers map[string]*Producer
	consumers map[string]*Consumer
}

func (svc *_service_) Build(options service.Options) (err error) {
	svc.log = options.Log
	config := Config{}
	configErr := options.Config.As(config)
	if configErr != nil {
		err = errors.Warning("nats: build failed").WithCause(configErr).WithMeta("service", _name)
		return
	}
	uri := strings.TrimSpace(config.URI)
	if uri == "" {
		err = errors.Warning("nats: build failed").WithCause(fmt.Errorf("uri is required")).WithMeta("service", _name)
		return
	}
	if config.Options == nil {
		conn, connErr := nats.Connect(uri)
		if connErr != nil {
			err = errors.Warning("nats: build failed").WithCause(connErr).WithMeta("service", _name)
			return
		}
		svc.conn = conn
	} else {
		opts := make([]nats.Option, 0, 1)

		if config.Options.Name != "" {
			opts = append(opts, nats.Name(config.Options.Name))
		}
		if config.Options.User != "" {
			opts = append(opts, nats.UserInfo(config.Options.User, config.Options.Password))
		}
		if config.Options.Token != "" {
			opts = append(opts, nats.Token(config.Options.Token))
		}
		if config.Options.EnableCompression {
			opts = append(opts, nats.Compression(config.Options.EnableCompression))
		}
		if config.Options.TimeoutSeconds > 0 {
			opts = append(opts, nats.Timeout(time.Duration(config.Options.TimeoutSeconds)*time.Second))
		}
		if config.Options.ClientTLS != nil {
			if config.Options.ClientTLS.CA != "" {
				opts = append(opts, nats.RootCAs(config.Options.ClientTLS.CA))
			}
			if config.Options.ClientTLS.Cert != "" {
				opts = append(opts, nats.ClientCert(config.Options.ClientTLS.Cert, config.Options.ClientTLS.Key))
			}
		}
		conn, connErr := nats.Connect(uri, opts...)
		if connErr != nil {
			err = errors.Warning("nats: build failed").WithCause(connErr).WithMeta("service", _name)
			return
		}
		svc.conn = conn
	}
	if config.Producers != nil {
		for subject, producerConfig := range config.Producers {
			if producerConfig == nil {
				continue
			}
			producer, producerErr := newProducer(svc.conn, subject, producerConfig)
			if producerErr != nil {
				err = errors.Warning("nats: build failed").WithCause(producerErr).WithMeta("service", _name)
				return
			}
			svc.producers[subject] = producer
		}
	}
	if config.Consumers != nil {
		for consumerName, consumerConfig := range config.Consumers {
			if consumerConfig == nil {
				continue
			}
			consumer, consumerErr := newConsumer(svc.conn, consumerName, svc.log.With("consumer", consumerName), consumerConfig)
			if consumerErr != nil {
				err = errors.Warning("nats: build failed").WithCause(consumerErr).WithMeta("service", _name)
				return
			}
			svc.consumers[consumerName] = consumer
		}
	}
	return
}

func (svc *_service_) Name() (name string) {
	name = _name
	return
}

func (svc *_service_) Internal() (internal bool) {
	return
}

func (svc *_service_) Components() (components map[string]service.Component) {
	return
}

func (svc *_service_) Document() (doc service.Document) {
	return
}

func (svc *_service_) Handle(ctx context.Context, fn string, argument service.Argument) (v interface{}, err errors.CodeError) {
	switch fn {
	case "publish":
		arg := PublishArgument{}
		scanErr := argument.As(&arg)
		if scanErr != nil {
			err = errors.BadRequest("nats: scan request argument failed").WithCause(scanErr).WithMeta("service", _name).WithMeta("fn", fn)
			return
		}
		producer, hasProducer := svc.producers[arg.Subject]
		if !hasProducer {
			err = errors.BadRequest(fmt.Sprintf("nats: %s producer was not found", arg.Subject)).WithMeta("service", _name).WithMeta("fn", fn)
			return
		}
		ok, publishErr := producer.Publish(ctx, arg.Body)
		if publishErr != nil {
			err = publishErr.WithMeta("service", _name).WithMeta("fn", fn)
			return
		}
		v = &PublishResult{
			Succeed: ok,
		}
	default:
		err = errors.NotFound("nats: fn was not found").WithMeta("service", _name).WithMeta("fn", fn)
		break
	}
	return
}

func (svc *_service_) Close() {
	if svc.consumers != nil && len(svc.consumers) > 0 {
		for _, consumer := range svc.consumers {
			_ = consumer.Close()
		}
	}
	if svc.producers != nil && len(svc.producers) > 0 {
		for _, producer := range svc.producers {
			_ = producer.Close()
		}
	}
	_ = svc.conn.Drain()
	svc.conn.Close()
	return
}

func (svc *_service_) Sharing() (ok bool) {
	return
}

func (svc *_service_) Listen(ctx context.Context) (err error) {
	if svc.consumers == nil || len(svc.consumers) == 0 {
		return
	}
	consumeErrCh := make(chan error, 1)
	for _, consumer := range svc.consumers {
		ctx = service.SetLog(ctx, svc.log.With("consumer", consumer.name))
		go func(ctx context.Context, consumer *Consumer, errCh chan error) {
			consumeErr := consumer.Consume(ctx)
			if consumeErr != nil {
				errCh <- consumeErr
			}
		}(ctx, consumer, consumeErrCh)
		select {
		case <-time.After(1 * time.Second):
			break
		case consumeErr := <-consumeErrCh:
			err = errors.Warning("nats: listen failed").WithCause(consumeErr).WithMeta("consumer", consumer.name)
			return
		}
	}
	return
}
