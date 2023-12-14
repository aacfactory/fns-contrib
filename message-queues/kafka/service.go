package kafka

import (
	"fmt"
	"github.com/aacfactory/errors"
	"github.com/aacfactory/fns/context"
	"github.com/aacfactory/fns/services"
	"github.com/segmentio/kafka-go"
	"github.com/segmentio/kafka-go/sasl"
	"github.com/segmentio/kafka-go/sasl/plain"
	"github.com/segmentio/kafka-go/sasl/scram"
	"net"
	"strings"
	"time"
)

var (
	endpointName = []byte("kafka")
)

type Options struct {
	consumers map[string]Consumer
}

type Option func(options *Options)

func WithReader(name string, consumer Consumer) Option {
	return func(options *Options) {
		options.consumers[name] = consumer
	}
}

func New(options ...Option) services.Service {
	opt := Options{
		consumers: make(map[string]Consumer),
	}
	for _, option := range options {
		option(&opt)
	}
	return &service{
		Abstract:  services.NewAbstract(string(endpointName), true),
		consumers: opt.consumers,
	}
}

type service struct {
	services.Abstract
	writers   map[string]*Writer
	consumers map[string]Consumer
	readers   map[string]*Reader
	cancel    context.CancelFunc
}

func (svc *service) Construct(options services.Options) (err error) {
	err = svc.Abstract.Construct(options)
	if err != nil {
		err = errors.Warning("kafka: construct failed").WithCause(err)
		return
	}
	config := Config{}
	configErr := options.Config.As(&config)
	if configErr != nil {
		err = errors.Warning("kafka: construct failed").WithCause(configErr)
		return
	}
	brokers := config.Brokers
	if brokers == nil || len(brokers) == 0 {
		err = errors.Warning("kafka: construct failed").WithCause(fmt.Errorf("brokers is required"))
		return
	}
	dialer := &kafka.Dialer{
		Timeout:   10 * time.Second,
		DualStack: true,
	}
	transport := &kafka.Transport{
		Dial: (&net.Dialer{
			Timeout: 3 * time.Second,
		}).DialContext,
	}
	username := strings.TrimSpace(config.Options.Username)
	if username != "" {
		password := strings.TrimSpace(config.Options.Password)
		if password == "" {
			err = errors.Warning("kafka: construct failed").WithCause(fmt.Errorf("password is required"))
			return
		}
		saslType := strings.ToLower(strings.TrimSpace(config.Options.SASLType))
		if saslType == "" {
			saslType = "plain"
		}
		var auth sasl.Mechanism
		switch saslType {
		case "plain":
			auth = plain.Mechanism{
				Username: username,
				Password: password,
			}
		case "scram":
			algo := strings.ToUpper(strings.TrimSpace(config.Options.Algo))
			var algorithm scram.Algorithm
			switch algo {
			case "SHA512":
				algorithm = scram.SHA512
			case "SHA256":
				algorithm = scram.SHA256
			default:
				err = errors.Warning("kafka: construct failed").WithCause(fmt.Errorf("algo is required"))
				return
			}
			var authErr error
			auth, authErr = scram.Mechanism(algorithm, username, password)
			if authErr != nil {
				err = errors.Warning("kafka: construct failed").WithCause(authErr)
				return
			}
		default:
			err = errors.Warning("kafka: construct failed").WithCause(fmt.Errorf("sasl type is invalid, plain and scram are supported"))
			return
		}
		dialer.SASLMechanism = auth
		transport.SASL = auth
	}
	if config.Options.ClientTLS.Enabled {
		clientTLS, clientTLSErr := config.Options.ClientTLS.Config()
		if clientTLSErr != nil {
			err = errors.Warning("kafka: construct failed").WithCause(clientTLSErr)
			return
		}
		dialer.TLS = clientTLS
		transport.TLS = clientTLS
	}
	if config.Options.DualStack {
		dialer.DualStack = true
	}
	if config.Options.TimeoutSeconds > 0 {
		dialer.Timeout = time.Duration(config.Options.TimeoutSeconds) * time.Second
	}
	clientId := strings.TrimSpace(config.Options.ClientId)
	if clientId != "" {
		dialer.ClientID = clientId
		transport.ClientID = clientId
	}
	for topic, writerConfig := range config.Writer {
		svc.writers[topic] = NewWriter(config.Brokers, topic, transport, svc.Log(), writerConfig)
	}
	for name, consumer := range svc.consumers {
		readerConfig, has := config.Reader[name]
		if !has {
			err = errors.Warning("kafka: construct failed").WithCause(fmt.Errorf("%s consumer has no config", name))
			return
		}
		reader := NewReader(config.Brokers, dialer, svc.Log(), readerConfig, consumer)
		svc.readers[name] = reader
	}
	svc.addFns()
	return
}

func (svc *service) Listen(ctx context.Context) (err error) {
	if svc.readers == nil || len(svc.readers) == 0 {
		return
	}
	ctx, svc.cancel = context.WithCancel(ctx)
	for _, reader := range svc.readers {
		go func(ctx context.Context, reader *Reader) {
			reader.ReadMessage(ctx)
		}(ctx, reader)
	}
	return
}

func (svc *service) Shutdown(_ context.Context) {
	if svc.cancel != nil {
		svc.cancel()
	}
	for _, writer := range svc.writers {
		_ = writer.raw.Close()
	}
	return
}

func (svc *service) addFns() {
	svc.AddFunction(&publishFn{
		writers: svc.writers,
	})
}
