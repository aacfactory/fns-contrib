package redis

import (
	"github.com/aacfactory/errors"
	"github.com/aacfactory/fns-contrib/databases/redis/configs"
	"github.com/aacfactory/fns/context"
	"github.com/aacfactory/fns/services"
	"github.com/redis/rueidis"
)

var (
	endpointName           = []byte("redis")
	commandFnName          = []byte("command")
	endpointNameContextKey = []byte("@fns:redis:endpoint:name")
)

func WithName(name string) Option {
	return func(options *Options) {
		if name == "" {
			return
		}
		options.name = name
	}
}

type Options struct {
	name string
}

type Option func(options *Options)

func New(options ...Option) services.Service {
	opt := Options{
		name: string(endpointName),
	}
	for _, option := range options {
		option(&opt)
	}
	return &service{
		Abstract: services.NewAbstract(opt.name, true),
	}
}

type service struct {
	services.Abstract
	client rueidis.Client
}

func (svc *service) Construct(options services.Options) (err error) {
	err = svc.Abstract.Construct(options)
	if err != nil {
		return
	}
	config := configs.Config{}
	configErr := options.Config.As(&config)
	if configErr != nil {
		err = errors.Warning("redis: service construct failed").WithCause(configErr).WithMeta("service", svc.Name())
		return
	}
	svc.client, err = config.Make()
	if err != nil {
		err = errors.Warning("redis: service construct failed").WithCause(err).WithMeta("service", svc.Name())
		return
	}
	svc.AddFunction(&commandHandler{
		client: svc.client,
	})
	return
}

func (svc *service) Shutdown(_ context.Context) {
	svc.client.Close()
}

func Use(ctx context.Context, endpointName []byte) context.Context {
	ctx.SetLocalValue(endpointNameContextKey, endpointName)
	return ctx
}

func used(ctx context.Context) []byte {
	name, _ := context.LocalValue[[]byte](ctx, endpointNameContextKey)
	return name
}
