package kafka

import (
	"fmt"
	"github.com/aacfactory/errors"
	"github.com/aacfactory/fns-contrib/message-queues/kafka/configs"
	"github.com/aacfactory/fns/context"
	"github.com/aacfactory/fns/services"
	"time"
)

var (
	endpointName = []byte("kafka")
)

type Options struct {
	consumeHandlers   map[string]ConsumeHandler
	consumeErrHandler ConsumeErrorHandler
}

type Option func(options *Options)

func WithConsumeHandler(name string, handler ConsumeHandler) Option {
	return func(options *Options) {
		options.consumeHandlers[name] = handler
	}
}

func WithConsumeErrorHandler(handler ConsumeErrorHandler) Option {
	return func(options *Options) {
		options.consumeErrHandler = handler
	}
}

func New(options ...Option) services.Listenable {
	opt := Options{
		consumeHandlers: make(map[string]ConsumeHandler),
	}
	for _, option := range options {
		option(&opt)
	}
	return &service{
		Abstract:          services.NewAbstract(string(endpointName), true),
		consumeHandlers:   opt.consumeHandlers,
		consumeErrHandler: opt.consumeErrHandler,
		producer:          nil,
		consumers:         make(map[string]Consumer),
	}
}

type service struct {
	services.Abstract
	consumeHandlers   map[string]ConsumeHandler
	consumeErrHandler ConsumeErrorHandler
	producer          *Producer
	consumers         map[string]Consumer
}

func (svc *service) Construct(options services.Options) (err error) {
	err = svc.Abstract.Construct(options)
	if err != nil {
		err = errors.Warning("kafka: construct failed").WithCause(err)
		return
	}
	config := configs.Config{}
	configErr := options.Config.As(&config)
	if configErr != nil {
		err = errors.Warning("kafka: construct failed").WithCause(configErr)
		return
	}
	opts, optsErr := config.Options(options.Id, options.Version, svc.Log())
	if optsErr != nil {
		err = errors.Warning("kafka: construct failed").WithCause(optsErr)
		return
	}
	if config.Producers.Enable {
		producerOpts, producerOptsErr := config.Producers.Options()
		if producerOptsErr != nil {
			err = errors.Warning("kafka: construct failed").WithCause(producerOptsErr)
			return
		}
		producerOpts = append(opts, producerOpts...)
		svc.producer, err = NewProducer(svc.Log().With("component", "producer"), config.Producers.Num, producerOpts)
		if err != nil {
			err = errors.Warning("kafka: construct failed").WithCause(err)
			return
		}
	}
	if config.Consumers != nil {
		for name, consumerConfig := range config.Consumers {
			handler, has := svc.consumeHandlers[name]
			if !has {
				err = errors.Warning("kafka: construct failed").WithCause(fmt.Errorf("%s consumer hander is not found", name))
				return
			}
			consumerOpts, consumerOptsErr := consumerConfig.Options()
			if consumerOptsErr != nil {
				err = errors.Warning("kafka: construct failed").WithCause(consumerOptsErr)
				return
			}
			consumerOpts = append(opts, consumerOpts...)
			consumerLog := svc.Log().With("component", "consumer").With("consumer", name)
			consumer, consumerErr := NewGroupConsumer(consumerLog, consumerConfig.MaxPollRecords, consumerConfig.PartitionBuffer, consumerOpts, handler, svc.consumeErrHandler)
			if consumerErr != nil {
				err = errors.Warning("kafka: construct failed").WithCause(consumerErr).WithMeta("consumer", name)
				return
			}
			svc.consumers[name] = consumer
		}
	}

	svc.addFns()
	return
}

func (svc *service) Listen(ctx context.Context) (err error) {
	if svc.consumers == nil || len(svc.consumers) == 0 {
		return
	}
	for _, consumer := range svc.consumers {
		errCn := make(chan error, 1)
		go func(ctx context.Context, consumer Consumer, errCh chan error) {
			lnErr := consumer.Listen(ctx)
			if lnErr != nil {
				errCn <- lnErr
			}
		}(ctx, consumer, errCn)
		select {
		case <-ctx.Done():
			break
		case err = <-errCn:
			break
		case <-time.After(3 * time.Second):
			break
		}
		if err != nil {
			break
		}
	}
	if err != nil {
		svc.Shutdown(ctx)
	}
	return
}

func (svc *service) Shutdown(ctx context.Context) {
	if svc.producer != nil {
		svc.producer.Shutdown(ctx)
	}
	if svc.consumers != nil {
		for _, consumer := range svc.consumers {
			consumer.Shutdown(ctx)
		}
		return
	}
	return
}

func (svc *service) addFns() {
	svc.AddFunction(&publishFn{
		producer: svc.producer,
	})
}
