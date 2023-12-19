package configs

import (
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
	case "all_isr":
		return kgo.AllISRAcks()
	default:
		return kgo.NoAck()
	}
}

type ProducerConfig struct {
	TransactionId       string        `json:"transactionId"`
	TransactionTimeout  time.Duration `json:"transactionTimeout"`
	Ack                 Ack           `json:"ack"`
	DisableIdempotency  bool          `json:"disableIdempotency"`
	MaxProduceInflight  int           `json:"maxProduceInflight"` // if idempotency is disabled, we allow a configurable max inflight
	Compressions        []Compression `json:"compressions"`
	MaxRecordBatchBytes string        `json:"maxRecordBatchBytes"`
	MaxBufferedRecords  int64         `json:"maxBufferedRecords"`
	MaxBufferedBytes    string        `json:"maxBufferedBytes"`
	Timeout             time.Duration `json:"timeout"`
	RecordRetries       int64         `json:"recordRetries"`
	MaxUnknownFailures  int64         `json:"maxUnknownFailures"`
	Linger              time.Duration `json:"linger"`
	RecordTimeout       time.Duration `json:"recordTimeout"`
	ManualFlushing      bool          `json:"manualFlushing"`
	TransactionBackoff  time.Duration `json:"transactionBackoff"`
	MissingTopicDelete  time.Duration `json:"missingTopicDelete"`
	Partitioner         *Partitioner  `json:"partitioner"`
	StopOnDataLoss      bool          `json:"stopOnDataLoss"`
}
