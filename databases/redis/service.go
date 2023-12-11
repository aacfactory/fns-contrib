package redis

import (
	"crypto/tls"
	"github.com/aacfactory/errors"
	"github.com/aacfactory/fns-contrib/databases/redis/configs"
	"github.com/aacfactory/fns/context"
	"github.com/aacfactory/fns/services"
	"github.com/redis/rueidis"
	"net"
)

var (
	endpointName           = []byte("redis")
	commandFnName          = []byte("command")
	cacheableFnName        = []byte("cacheable")
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

func WithSendToReplicas(fn func(cmd rueidis.Completed) bool) Option {
	return func(options *Options) {
		options.SendToReplicas = fn
	}
}

func WithAuthCredentials(fn func(rueidis.AuthCredentialsContext) (rueidis.AuthCredentials, error)) Option {
	return func(options *Options) {
		options.AuthCredentialsFn = fn
	}
}

func WithDialer(dialer *net.Dialer) Option {
	return func(options *Options) {
		options.Dialer = dialer
	}
}

func WithDialFn(fn func(string, *net.Dialer, *tls.Config) (conn net.Conn, err error)) Option {
	return func(options *Options) {
		options.DialFn = fn
	}
}

func WithSentinelDialer(dialer *net.Dialer) Option {
	return func(options *Options) {
		options.SentinelDialer = dialer
	}
}

func WithNewCacheStoreFn(fn rueidis.NewCacheStoreFn) Option {
	return func(options *Options) {
		options.NewCacheStoreFn = fn
	}
}

type Options struct {
	name string
	configs.Options
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
		opt:      opt.Options,
	}
}

type service struct {
	services.Abstract
	opt    configs.Options
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
	svc.client, err = config.Make(svc.opt)
	if err != nil {
		err = errors.Warning("redis: service construct failed").WithCause(err).WithMeta("service", svc.Name())
		return
	}
	handler := &commandHandler{
		client: svc.client,
	}
	svc.AddFunction(handler)
	svc.AddFunction(&cacheableHandler{
		client:       svc.client,
		disableCache: config.DisableCache,
		handler:      handler,
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
