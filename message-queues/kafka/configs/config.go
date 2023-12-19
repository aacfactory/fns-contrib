package configs

import (
	"github.com/aacfactory/errors"
	"github.com/aacfactory/fns/commons/bytex"
	"github.com/aacfactory/fns/commons/versions"
	"github.com/aacfactory/logs"
	"github.com/twmb/franz-go/pkg/kgo"
	"time"
)

type Generic struct {
	Brokers                []string      `json:"brokers"`
	DialTimeout            time.Duration `json:"dialTimeout"`
	RequestTimeoutOverhead time.Duration `json:"requestTimeoutOverhead"`
	ConnIdleTimeout        time.Duration `json:"connIdleTimeout"`
	Retries                int           `json:"retries"`
	RetryTimeout           time.Duration `json:"retryTimeout"`
	MaxBrokerWriteBytes    string        `json:"maxBrokerWriteBytes"`
	MaxBrokerReadBytes     string        `json:"maxBrokerReadBytes"`
	AllowAutoTopicCreation bool          `json:"allowAutoTopicCreation"`
	Meta                   MetaConfig    `json:"meta"`
	SASL                   SASLConfig    `json:"sasl"`
	SSL                    SSLConfig     `json:"ssl"`
}

func (config *Generic) Options(id string, version versions.Version, log logs.Logger) (v []kgo.Opt, err error) {
	opts := make([]kgo.Opt, 0, 1)
	// brokers
	if len(config.Brokers) == 0 {
		err = errors.Warning("brokers are required")
		return
	}
	opts = append(opts, kgo.SeedBrokers(config.Brokers...))
	// DialTimeout
	if config.DialTimeout > 0 {
		opts = append(opts, kgo.DialTimeout(config.DialTimeout))
	}
	// RequestTimeoutOverhead
	if config.RequestTimeoutOverhead > 0 {
		opts = append(opts, kgo.RequestTimeoutOverhead(config.RequestTimeoutOverhead))
	}
	// ConnIdleTimeout
	if config.ConnIdleTimeout > 0 {
		opts = append(opts, kgo.ConnIdleTimeout(config.ConnIdleTimeout))
	}
	// id
	opts = append(opts, kgo.ClientID(id))
	// version
	opts = append(opts, kgo.SoftwareNameAndVersion("fns", version.String()))
	// Retries
	if config.Retries > 0 {
		opts = append(opts, kgo.RequestRetries(config.Retries))
	}
	// RetryTimeout
	if config.RetryTimeout > 0 {
		opts = append(opts, kgo.RetryTimeout(config.RetryTimeout))
	}
	// MaxBrokerWriteBytes
	if maxBrokerWriteBytes := config.MaxBrokerWriteBytes; maxBrokerWriteBytes != "" {
		n, nErr := bytex.ParseBytes(maxBrokerWriteBytes)
		if nErr != nil {
			err = errors.Warning("maxBrokerWriteBytes is invalid").WithCause(nErr)
			return
		}
		opts = append(opts, kgo.BrokerMaxWriteBytes(int32(n)))
	}
	// MaxBrokerReadBytes
	if maxBrokerReadBytes := config.MaxBrokerReadBytes; maxBrokerReadBytes != "" {
		n, nErr := bytex.ParseBytes(maxBrokerReadBytes)
		if nErr != nil {
			err = errors.Warning("maxBrokerReadBytes is invalid").WithCause(nErr)
			return
		}
		opts = append(opts, kgo.BrokerMaxReadBytes(int32(n)))
	}
	// AllowAutoTopicCreation
	if config.AllowAutoTopicCreation {
		opts = append(opts, kgo.AllowAutoTopicCreation())
	}
	// Meta
	if config.Meta.MaxAge > 0 {
		opts = append(opts, kgo.MetadataMaxAge(config.Meta.MaxAge))
	}
	if config.Meta.MinAge > 0 {
		opts = append(opts, kgo.MetadataMinAge(config.Meta.MinAge))
	}
	// SASL
	if config.SASL.Name != "" {
		mechanism, saslErr := config.SASL.Config(log.With("sasl", config.SASL.Name))
		if saslErr != nil {
			err = errors.Warning("sasl is invalid").WithCause(saslErr)
			return
		}
		opts = append(opts, kgo.SASL(convert(mechanism)))
	}
	// ssl
	if config.SSL.Enable {
		tlsConfig, tlsErr := config.SSL.Load()
		if tlsErr != nil {
			err = errors.Warning("ssl is invalid").WithCause(tlsErr)
			return
		}
		opts = append(opts, kgo.DialTLSConfig(tlsConfig))
	}
	// log
	opts = append(opts, kgo.WithLogger(&Logger{raw: log}))
	return
}

func (config *Generic) NewClient(id string, version versions.Version, log logs.Logger) (v *kgo.Client, err error) {
	opts, optsErr := config.Options(id, version, log)
	if optsErr != nil {
		err = errors.Warning("kafka: create client failed").WithCause(optsErr)
		return
	}
	v, err = kgo.NewClient(opts...)
	if err != nil {
		err = errors.Warning("kafka: create client failed").WithCause(err)
		return
	}
	return
}

type Config struct {
	Generic
	Producers ProducerConfig            `json:"producers"`
	Consumers map[string]ConsumerConfig `json:"consumers"`
}
