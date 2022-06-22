package kafka

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"github.com/aacfactory/errors"
	"github.com/aacfactory/fns/service"
	"github.com/aacfactory/logs"
	"github.com/segmentio/kafka-go"
	"github.com/segmentio/kafka-go/sasl"
	"github.com/segmentio/kafka-go/sasl/plain"
	"github.com/segmentio/kafka-go/sasl/scram"
	"io/ioutil"
	"strings"
	"time"
)

const (
	name = "kafka"
)

func Service() service.Service {
	return &_service_{
		log:       nil,
		producers: make(map[string]*Producer),
		consumers: make(map[string]*Consumer),
	}
}

type _service_ struct {
	log       logs.Logger
	producers map[string]*Producer
	consumers map[string]*Consumer
}

func (svc *_service_) Build(options service.Options) (err error) {
	svc.log = options.Log
	config := Config{}
	configErr := options.Config.As(config)
	if configErr != nil {
		err = errors.Warning("kafka: build failed").WithCause(configErr).WithMeta("service", name)
		return
	}
	brokers := config.Brokers
	if brokers == nil || len(brokers) == 0 {
		err = errors.Warning("kafka: build failed").WithCause(fmt.Errorf("brokers is required")).WithMeta("service", name)
		return
	}
	if config.Options == nil {
		dialer := kafka.DefaultDialer
		transport := kafka.DefaultTransport.(*kafka.Transport)
		username := strings.TrimSpace(config.Options.Username)
		if username != "" {
			password := strings.TrimSpace(config.Options.Password)
			if password == "" {
				err = errors.Warning("kafka: build failed").WithCause(fmt.Errorf("password is required")).WithMeta("service", name)
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
					err = errors.Warning("kafka: build failed").WithCause(fmt.Errorf("algo is required")).WithMeta("service", name)
					return
				}
				var authErr error
				auth, authErr = scram.Mechanism(algorithm, username, password)
				if authErr != nil {
					err = errors.Warning("kafka: build failed").WithCause(authErr).WithMeta("service", name)
					return
				}
			default:
				err = errors.Warning("kafka: build failed").WithCause(fmt.Errorf("sasl type is invalid, plain and scram are supported")).WithMeta("service", name)
				return
			}
			dialer.SASLMechanism = auth
			transport.SASL = auth
		}
		if config.Options.ClientTLS != nil {
			clientTLS := &tls.Config{}
			caPath := strings.TrimSpace(config.Options.ClientTLS.CA)
			if caPath != "" {
				caPEM, caErr := ioutil.ReadFile(caPath)
				if caErr != nil {
					err = errors.Warning("kafka: build failed").WithCause(caErr).WithMeta("service", name)
					return
				}
				rootCAs := x509.NewCertPool()
				if !rootCAs.AppendCertsFromPEM(caPEM) {
					err = errors.Warning("kafka: build failed").WithCause(fmt.Errorf("append root ca pool failed")).WithMeta("service", name)
					return
				}
				clientTLS.RootCAs = rootCAs
			}
			certPath := strings.TrimSpace(config.Options.ClientTLS.Cert)
			if certPath == "" {
				err = errors.Warning("kafka: build failed").WithCause(fmt.Errorf("client cert file path is required")).WithMeta("service", name)
				return
			}
			keyPath := strings.TrimSpace(config.Options.ClientTLS.Key)
			if keyPath == "" {
				err = errors.Warning("kafka: build failed").WithCause(fmt.Errorf("client key file path is required")).WithMeta("service", name)
				return
			}
			clientCertificate, clientCertificateErr := tls.LoadX509KeyPair(certPath, keyPath)
			if clientCertificateErr != nil {
				err = errors.Warning("kafka: build failed").WithCause(clientCertificateErr).WithMeta("service", name)
				return
			}
			clientTLS.Certificates = []tls.Certificate{clientCertificate}
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
	}
	if config.Producers != nil {
		for producerTopic, producerConfig := range config.Producers {
			if producerConfig == nil {
				continue
			}
			producer, producerErr := newProducer(producerTopic, svc.log, producerConfig, brokers)
			if producerErr != nil {
				err = errors.Warning("kafka: build failed").WithCause(producerErr).WithMeta("service", name)
				return
			}
			svc.producers[producerTopic] = producer
		}
	}
	if config.Consumers != nil {
		for consumerTopic, consumerConfig := range config.Consumers {
			if consumerConfig == nil {
				continue
			}
			consumer, consumerErr := newConsumer(consumerTopic, svc.log, consumerConfig, brokers)
			if consumerErr != nil {
				err = errors.Warning("kafka: build failed").WithCause(consumerErr).WithMeta("service", name)
				return
			}
			svc.consumers[consumerTopic] = consumer
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
	case "publish":
		arg := PublishArgument{}
		scanErr := argument.As(&arg)
		if scanErr != nil {
			err = errors.BadRequest("kafka: scan request argument failed").WithCause(scanErr).WithMeta("service", name).WithMeta("fn", fn)
			return
		}
		producer, hasProducer := svc.producers[arg.Topic]
		if !hasProducer {
			err = errors.BadRequest(fmt.Sprintf("kafka: %s producer was not found", arg.Topic)).WithMeta("service", name).WithMeta("fn", fn)
			return
		}
		ok, publishErr := producer.Publish(ctx, kafka.Message{
			Topic: arg.Topic,
			Key:   []byte(arg.Key),
			Value: arg.Body,
		})
		if publishErr != nil {
			err = publishErr.WithMeta("service", name).WithMeta("fn", fn)
			return
		}
		v = &PublishResult{
			Succeed: ok,
		}
	default:
		err = errors.NotFound("kafka: fn was not found").WithMeta("service", name).WithMeta("fn", fn)
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
		ctx = service.SetLog(ctx, svc.log.With("consumer", consumer.topic))
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
			err = errors.Warning("kafka: listen failed").WithCause(consumeErr).WithMeta("consumer", consumer.topic)
			return
		}
	}
	return
}
