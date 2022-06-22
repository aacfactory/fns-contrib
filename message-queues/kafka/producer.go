package kafka

import (
	"context"
	"fmt"
	"github.com/aacfactory/errors"
	"github.com/aacfactory/logs"
	"github.com/segmentio/kafka-go"
	"strings"
)

func newProducer(topic string, log logs.Logger, config *ProducerConfig, brokers []string) (producer *Producer, err error) {
	log = log.With("producer", topic)
	topic = strings.TrimSpace(topic)
	if topic == "" {
		err = errors.Warning(fmt.Sprintf("kafka: new %s producer failed", topic)).WithCause(fmt.Errorf("topic is required"))
		return
	}
	var balancer kafka.Balancer
	switch config.Balancer {
	case "round_robin":
		balancer = &kafka.RoundRobin{}
	case "hash":
		balancer = &kafka.Hash{}
	case "reference_hash":
		balancer = &kafka.ReferenceHash{}
	case "crc32":
		balancer = &kafka.CRC32Balancer{}
	case "murmur2":
		balancer = &kafka.Murmur2Balancer{}
	case "least":
		balancer = &kafka.LeastBytes{}
	default:
		balancer = &kafka.LeastBytes{}
		break
	}
	requiredAck := kafka.RequireNone
	switch config.RequiredAck {
	case "one":
		requiredAck = kafka.RequireOne
	case "all":
		requiredAck = kafka.RequireAll
	default:
		break
	}
	batchSize := config.BatchSize
	if batchSize < 0 {
		batchSize = 0
	}
	compression := kafka.Compression(0)
	switch config.Compression {
	case "gzip":
		compression = kafka.Gzip
	case "snappy":
		compression = kafka.Snappy
	case "lz4":
		compression = kafka.Lz4
	case "zstd":
		compression = kafka.Zstd
	default:
		break
	}
	w := &kafka.Writer{
		Addr:         kafka.TCP(brokers...),
		Topic:        topic,
		Balancer:     balancer,
		BatchSize:    batchSize,
		RequiredAcks: requiredAck,
		Async:        config.Async,
		Compression:  compression,
		Logger: &Printf{
			Core: log,
		},
		ErrorLogger: &Printf{
			Core: log,
		},
	}
	producer = &Producer{
		log:    log,
		writer: w,
	}
	return
}

type Producer struct {
	log    logs.Logger
	writer *kafka.Writer
}

func (producer *Producer) Publish(ctx context.Context, msg kafka.Message) (ok bool, err errors.CodeError) {
	if msg.Key == nil || len(msg.Key) == 0 {
		err = errors.ServiceError("kafka: publish failed").WithCause(fmt.Errorf("message key is nil or empty"))
		return
	}
	if msg.Value == nil || len(msg.Value) == 0 {
		err = errors.ServiceError("kafka: publish failed").WithCause(fmt.Errorf("message value is nil or empty"))
		return
	}
	publishErr := producer.writer.WriteMessages(ctx, msg)
	if publishErr != nil {
		err = errors.ServiceError("kafka: publish failed").WithCause(publishErr)
		return
	}
	return
}

func (producer *Producer) Close() (err error) {
	_ = producer.writer.Close()
	return
}
