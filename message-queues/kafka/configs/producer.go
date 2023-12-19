package configs

import (
	"github.com/aacfactory/errors"
	"github.com/aacfactory/fns/commons/bytex"
	"github.com/twmb/franz-go/pkg/kgo"
	"time"
)

type Ack string

func (ack Ack) Config() kgo.Acks {
	switch ack {
	case "no":
		return kgo.NoAck()
	case "leader":
		return kgo.LeaderAck()
	case "all":
		return kgo.AllISRAcks()
	default:
		return kgo.NoAck()
	}
}

type ProducerConfig struct {
	Enable                bool          `json:"enable"`
	Num                   int           `json:"num"`
	Ack                   Ack           `json:"ack"`
	DisableIdempotency    bool          `json:"disableIdempotency"`
	MaxInflight           int           `json:"maxInflight"` // if idempotency is disabled, we allow a configurable max inflight
	Compressions          []Compression `json:"compressions"`
	MaxRecordBatchBytes   string        `json:"maxRecordBatchBytes"`
	MaxBufferedRecords    int           `json:"maxBufferedRecords"`
	MaxBufferedBytes      string        `json:"maxBufferedBytes"`
	Timeout               time.Duration `json:"timeout"`
	RecordRetries         int           `json:"recordRetries"`
	MaxUnknownFailures    int           `json:"maxUnknownFailures"`
	Linger                time.Duration `json:"linger"`
	RecordDeliveryTimeout time.Duration `json:"recordDeliveryTimeout"`
	Partitioner           *Partitioner  `json:"partitioner"`
}

func (config *ProducerConfig) Options() (opts []kgo.Opt, err error) {
	if !config.Enable {
		return
	}
	opts = make([]kgo.Opt, 0, 1)
	// ack
	opts = append(opts, kgo.RequiredAcks(config.Ack.Config()))
	// disableIdempotency
	if config.DisableIdempotency {
		opts = append(opts, kgo.DisableIdempotentWrite())
		if config.MaxInflight > 0 {
			opts = append(opts, kgo.MaxProduceRequestsInflightPerBroker(config.MaxInflight))
		}
	} else {
		opts = append(opts, kgo.RequiredAcks(kgo.AllISRAcks()))
	}
	// compressions
	if compressionLen := len(config.Compressions); compressionLen > 0 {
		compressions := make([]kgo.CompressionCodec, compressionLen)
		for _, compression := range config.Compressions {
			compressions = append(compressions, compression.Config())
		}
		opts = append(opts, kgo.ProducerBatchCompression(compressions...))
	}
	// maxRecordBatchBytes
	if maxRecordBatchBytes := config.MaxRecordBatchBytes; maxRecordBatchBytes != "" {
		n, nErr := bytex.ParseBytes(maxRecordBatchBytes)
		if nErr != nil {
			err = errors.Warning("kafka: invalid maxRecordBatchBytes").WithCause(nErr)
			return
		}
		opts = append(opts, kgo.ProducerBatchMaxBytes(int32(n)))
	}
	// maxBufferedRecords
	if config.MaxBufferedRecords > 0 {
		opts = append(opts, kgo.MaxBufferedRecords(config.MaxBufferedRecords))
	}

	// maxBufferedBytes
	if maxBufferedBytes := config.MaxBufferedBytes; maxBufferedBytes != "" {
		n, nErr := bytex.ParseBytes(maxBufferedBytes)
		if nErr != nil {
			err = errors.Warning("kafka: invalid maxBufferedBytes").WithCause(nErr)
			return
		}
		opts = append(opts, kgo.MaxBufferedBytes(int(n)))
	}
	// timeout
	if config.Timeout > 0 {
		opts = append(opts, kgo.ProduceRequestTimeout(config.Timeout))
	}
	// recordRetries
	if config.RecordRetries > 0 {
		opts = append(opts, kgo.RecordRetries(config.RecordRetries))
	}
	// maxUnknownFailures
	if config.MaxUnknownFailures > 0 {
		opts = append(opts, kgo.UnknownTopicRetries(config.RecordRetries))
	}
	// linger
	if config.Linger > 0 {
		opts = append(opts, kgo.ProducerLinger(config.Linger))
	}
	// recordDeliveryTimeout
	if config.RecordDeliveryTimeout > 0 {
		opts = append(opts, kgo.RecordDeliveryTimeout(config.RecordDeliveryTimeout))
	}
	// partitioner
	if config.Partitioner != nil {
		partitioner, partitionerErr := config.Partitioner.Config()
		if partitionerErr != nil {
			err = errors.Warning("kafka: invalid partitioner").WithCause(partitionerErr)
			return
		}
		opts = append(opts, kgo.RecordPartitioner(partitioner))
	}
	return
}
