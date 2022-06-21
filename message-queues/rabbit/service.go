package rabbit

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"github.com/aacfactory/errors"
	"github.com/aacfactory/fns/service"
	"github.com/aacfactory/logs"
	amqp "github.com/rabbitmq/amqp091-go"
	"io/ioutil"
	"strings"
	"time"
)

const (
	name = "rabbitmq"
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
	conn      *amqp.Connection
	producers map[string]*Producer
	consumers map[string]*Consumer
}

func (svc *_service_) Build(options service.Options) (err error) {
	svc.log = options.Log
	config := Config{}
	configErr := options.Config.As(config)
	if configErr != nil {
		err = errors.Warning("rabbitmq: build failed").WithCause(configErr).WithMeta("service", name)
		return
	}
	uri := strings.TrimSpace(config.URI)
	if uri == "" {
		err = errors.Warning("rabbitmq: build failed").WithCause(fmt.Errorf("uri is required")).WithMeta("service", name)
		return
	}
	if config.Options == nil {
		conn, dialErr := amqp.Dial(uri)
		if dialErr != nil {
			err = errors.Warning("rabbitmq: build failed").WithCause(dialErr).WithMeta("service", name)
			return
		}
		svc.conn = conn
	} else {
		amqpConfig := amqp.Config{
			SASL:            nil,
			Vhost:           strings.TrimSpace(config.Options.Vhost),
			ChannelMax:      config.Options.ChannelMax,
			FrameSize:       config.Options.FrameSize,
			Heartbeat:       time.Duration(config.Options.HeartbeatSeconds) * time.Second,
			TLSClientConfig: nil,
			Properties:      nil,
			Locale:          strings.TrimSpace(config.Options.Locale),
			Dial:            nil,
		}
		if config.Options.AMQPlainAuth != nil {
			amqpConfig.SASL = []amqp.Authentication{config.Options.AMQPlainAuth}
		}
		if config.Options.ClientTLS != nil {
			clientTLS := &tls.Config{}
			caPath := strings.TrimSpace(config.Options.ClientTLS.CA)
			if caPath != "" {
				caPEM, caErr := ioutil.ReadFile(caPath)
				if caErr != nil {
					err = errors.Warning("rabbitmq: build failed").WithCause(caErr).WithMeta("service", name)
					return
				}
				rootCAs := x509.NewCertPool()
				if !rootCAs.AppendCertsFromPEM(caPEM) {
					err = errors.Warning("rabbitmq: build failed").WithCause(fmt.Errorf("append root ca pool failed")).WithMeta("service", name)
					return
				}
				clientTLS.RootCAs = rootCAs
			}
			certPath := strings.TrimSpace(config.Options.ClientTLS.Cert)
			if certPath == "" {
				err = errors.Warning("rabbitmq: build failed").WithCause(fmt.Errorf("client cert file path is required")).WithMeta("service", name)
				return
			}
			keyPath := strings.TrimSpace(config.Options.ClientTLS.Key)
			if keyPath == "" {
				err = errors.Warning("rabbitmq: build failed").WithCause(fmt.Errorf("client key file path is required")).WithMeta("service", name)
				return
			}
			clientCertificate, clientCertificateErr := tls.LoadX509KeyPair(certPath, keyPath)
			if clientCertificateErr != nil {
				err = errors.Warning("rabbitmq: build failed").WithCause(clientCertificateErr).WithMeta("service", name)
				return
			}
			clientTLS.Certificates = []tls.Certificate{clientCertificate}
			clientTLS.InsecureSkipVerify = config.Options.ClientTLS.InsecureSkipVerify
			amqpConfig.TLSClientConfig = clientTLS
		}
		conn, dialErr := amqp.DialConfig(uri, amqpConfig)
		if dialErr != nil {
			err = errors.Warning("rabbitmq: build failed").WithCause(dialErr).WithMeta("service", name)
			return
		}
		svc.conn = conn
	}
	if config.Producers != nil {
		for producerName, producerConfig := range config.Producers {
			if producerConfig == nil {
				continue
			}
			producer, producerErr := newProducer(svc.conn, producerName, producerConfig)
			if producerErr != nil {
				err = errors.Warning("rabbitmq: build failed").WithCause(producerErr).WithMeta("service", name)
				return
			}
			svc.producers[producerName] = producer
		}
	}
	if config.Consumers != nil {
		for consumerName, consumerConfig := range config.Consumers {
			if consumerConfig == nil {
				continue
			}
			consumer, consumerErr := newConsumer(svc.conn, consumerName, svc.log.With("consumer", consumerName), consumerConfig)
			if consumerErr != nil {
				err = errors.Warning("rabbitmq: build failed").WithCause(consumerErr).WithMeta("service", name)
				return
			}
			svc.consumers[consumerName] = consumer
		}
	}
	return
}

func (svc *_service_) Name() string {
	return name
}

func (svc *_service_) Internal() (internal bool) {
	internal = true
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
	case "produce":
		arg := ProduceArgument{}
		scanErr := argument.As(&arg)
		if scanErr != nil {
			err = errors.BadRequest("rabbitmq: scan request argument failed").WithCause(scanErr).WithMeta("service", name).WithMeta("fn", fn)
			return
		}
		producer, hasProducer := svc.producers[arg.Name]
		if !hasProducer {
			err = errors.BadRequest(fmt.Sprintf("rabbitmq: %s producer was not found", arg.Name)).WithMeta("service", name).WithMeta("fn", fn)
			return
		}
		ok, publishErr := producer.Publish(ctx, &producerMessage{
			ContentType_: "text/json",
			Body_:        arg.Body,
		})
		if publishErr != nil {
			err = publishErr.WithMeta("service", name).WithMeta("fn", fn)
			return
		}
		v = &ProduceResult{
			Succeed: ok,
		}
	default:
		err = errors.NotFound("rabbitmq: fn was not found").WithMeta("service", name).WithMeta("fn", fn)
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
	_ = svc.conn.Close()
	return
}

func (svc *_service_) Sharing() (ok bool) {
	ok = false
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
			err = errors.Warning("rabbitmq: listen failed").WithCause(consumeErr).WithMeta("consumer", consumer.name)
			return
		}
	}
	return
}
