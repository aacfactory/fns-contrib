package configs

import (
	"github.com/aacfactory/errors"
	"github.com/aacfactory/logs"
	"github.com/twmb/franz-go/pkg/kgo"
	"time"
)

type Generic struct {
	Brokers                []string      `json:"brokers"`
	DialTimeout            time.Duration `json:"dialTimeout"`
	RequestTimeoutOverhead time.Duration `json:"requestTimeoutOverhead"`
	ConnIdleTimeout        time.Duration `json:"connIdleTimeout"`
	Id                     string        `json:"id"`
	SoftwareName           string        `json:"softwareName"`
	SoftwareVersion        string        `json:"softwareVersion"`
	RetryBackoff           time.Duration `json:"retryBackoff"`
	Retries                int           `json:"retries"`
	RetryTimeout           time.Duration `json:"retryTimeout"`
	MaxBrokerWriteBytes    string        `json:"maxBrokerWriteBytes"`
	MaxBrokerReadBytes     string        `json:"maxBrokerReadBytes"`
	AllowAutoTopicCreation bool          `json:"allowAutoTopicCreation"`
	Meta                   MetaConfig    `json:"meta"`
	SASL                   SASLConfig    `json:"sasl"`
	SSL                    SSLConfig     `json:"ssl"`
}

func (config *Generic) Options(log logs.Logger) (v []kgo.Opt, err error) {
	opts := make([]kgo.Opt, 0, 1)

	// log
	opts = append(opts, kgo.WithLogger(&Logger{raw: log}))
	return
}

func (config *Generic) NewClient(log logs.Logger) (v *kgo.Client, err error) {
	opts, optsErr := config.Options(log)
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
	Producers map[string]ProducerConfig `json:"producers"`
	Consumers map[string]ConsumerConfig `json:"consumers"`
}
