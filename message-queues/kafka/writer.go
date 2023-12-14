package kafka

import (
	"context"
	"fmt"
	"github.com/aacfactory/errors"
	"github.com/aacfactory/logs"
	"github.com/segmentio/kafka-go"
	"time"
)

type WriteMessage struct {
	Key     []byte
	Body    []byte
	Headers Headers
	Offset  int64
	Time    time.Time
}

func (msg WriteMessage) AddHeader(key string, value []byte) WriteMessage {
	msg.Headers = append(msg.Headers, Header{
		Key:   key,
		Value: value,
	})
	return msg
}

func (msg WriteMessage) SetOffset(offset int64) WriteMessage {
	msg.Offset = offset
	return msg
}

func (msg WriteMessage) SetTime(createAT time.Time) WriteMessage {
	msg.Time = createAT
	return msg
}

func NewWriter(brokers []string, topic string, tr *kafka.Transport, log logs.Logger, config WriterConfig) (w *Writer) {
	raw := &kafka.Writer{
		Addr:         kafka.TCP(brokers...),
		Topic:        topic,
		Balancer:     config.Balance(),
		MaxAttempts:  config.MaxAttempts,
		BatchSize:    config.BatchSize,
		BatchBytes:   config.BatchBytes,
		RequiredAcks: config.Ack(),
		Async:        config.Async,
		Compression:  config.CompressionKind(),
		Logger: &Printf{
			Core: log.With("kafka", "writer"),
		},
		ErrorLogger: &Printf{
			Core: log.With("kafka", "writer"),
		},
		Transport:              tr,
		AllowAutoTopicCreation: true,
	}
	w = &Writer{
		raw:   raw,
		topic: topic,
	}
	return
}

type Writer struct {
	raw   *kafka.Writer
	topic string
}

func (w *Writer) Write(ctx context.Context, messages []WriteMessage) (err error) {
	if len(messages) == 0 {
		err = errors.Warning("kafka: write failed").WithCause(fmt.Errorf("messages are required"))
		return
	}
	mm := make([]kafka.Message, 0, len(messages))
	for _, message := range messages {
		if len(message.Key) == 0 {
			err = errors.Warning("kafka: write failed").WithCause(fmt.Errorf("key is required"))
			return
		}
		if len(message.Body) == 0 {
			err = errors.Warning("kafka: write failed").WithCause(fmt.Errorf("body is required"))
			return
		}
		mm = append(mm, kafka.Message{
			Topic:         w.topic,
			Partition:     0,
			Offset:        message.Offset,
			HighWaterMark: 0,
			Key:           message.Key,
			Value:         message.Body,
			Headers:       message.Headers.ConvertToKafkaHeaders(),
			Time:          message.Time,
		})
	}
	err = w.raw.WriteMessages(ctx, mm...)
	if err != nil {
		err = errors.Warning("kafka: write failed").WithCause(err)
		return
	}
	return
}
