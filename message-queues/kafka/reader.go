package kafka

import (
	"github.com/aacfactory/errors"
	"github.com/aacfactory/fns/context"
	"github.com/aacfactory/logs"
	"github.com/segmentio/kafka-go"
	"github.com/twmb/franz-go/pkg/kgo"
	"time"
)

type consumeMessage struct {
	raw       kgo.Record
	committer MessageCommitter
}

func (msg consumeMessage) Topic() (topic string) {
	topic = msg.raw.Topic
	return
}

func (msg consumeMessage) Key() (key []byte) {
	key = msg.raw.Key
	return
}

func (msg consumeMessage) Headers() (headers Headers) {
	headers = make(Headers, 0)
	for _, header := range msg.raw.Headers {
		headers = append(headers, Header{
			Key:   header.Key,
			Value: header.Value,
		})
	}
	return
}

func (msg consumeMessage) Body() (body []byte) {
	body = msg.raw.Value
	return
}

func (msg consumeMessage) Partition() (no int32) {
	no = msg.raw.Partition
	return
}

func (msg consumeMessage) Offset() (offset int64) {
	offset = msg.raw.Offset
	return
}

func (msg consumeMessage) Time() (v time.Time) {
	v = msg.raw.Timestamp
	return
}

func (msg consumeMessage) Commit(ctx context.Context) (err error) {
	err = msg.committer.Commit(ctx, msg.raw)
	return
}

func NewReader(brokers []string, dialer *kafka.Dialer, log logs.Logger, config ReaderConfig, consumer Consumer) (r *Reader) {
	topic := ""
	group := config.GroupId != ""
	if !group {
		topic = config.Topics[0]
	}
	raw := kafka.NewReader(kafka.ReaderConfig{
		Brokers:                brokers,
		GroupID:                config.GroupId,
		GroupTopics:            config.Topics,
		Topic:                  topic,
		Partition:              config.Partition,
		Dialer:                 dialer,
		QueueCapacity:          config.QueueCapacity,
		MinBytes:               config.MinBytes,
		MaxBytes:               config.MaxBytes,
		MaxWait:                config.MaxWait,
		ReadLagInterval:        config.ReadLagInterval,
		GroupBalancers:         config.KafkaGroupBalancers(),
		HeartbeatInterval:      config.HeartbeatInterval,
		CommitInterval:         config.CommitInterval,
		PartitionWatchInterval: config.PartitionWatchInterval,
		WatchPartitionChanges:  config.WatchPartitionChanges,
		SessionTimeout:         config.SessionTimeout,
		RebalanceTimeout:       config.RebalanceTimeout,
		JoinGroupBackoff:       config.JoinGroupBackoff,
		RetentionTime:          config.RetentionTime,
		StartOffset:            config.StartOffset,
		ReadBackoffMin:         config.ReadBackoffMin,
		ReadBackoffMax:         config.ReadBackoffMax,
		Logger: &Printf{
			Core: log.With("kafka", "reader"),
		},
		ErrorLogger: &Printf{
			Core: log.With("kafka", "reader"),
		},
		IsolationLevel:        config.Isolation(),
		MaxAttempts:           config.MaxAttempts,
		OffsetOutOfRangeError: config.OffsetOutOfRangeError,
	})

	var committer MessageCommitter
	if !config.AutoCommit {
		committer = &ExplicitMessageCommitter{
			reader: raw,
		}
	} else {
		committer = &NoopMessageCommitter{}
	}

	r = &Reader{
		log:       log,
		raw:       raw,
		group:     true,
		committer: committer,
		consumer:  consumer,
	}
	return
}

type Reader struct {
	log       logs.Logger
	raw       *kafka.Reader
	group     bool
	committer MessageCommitter
	consumer  Consumer
}

func (reader *Reader) ReadMessage(ctx context.Context) {
	stopped := false
	for {
		select {
		case <-ctx.Done():
			stopped = true
			break
		default:
			raw, fetchErr := reader.raw.FetchMessage(ctx)
			if fetchErr != nil {
				if reader.log.ErrorEnabled() {
					reader.log.Error().Cause(errors.Warning("kafka: read failed").WithCause(fetchErr)).
						With("kafka", "read").Message("kafka: read failed")
				}
				return
			}
			msg := consumeMessage{
				raw:       raw,
				committer: reader.committer,
			}
			reader.consumer.Handle(ctx, msg)
		}
		if stopped {
			break
		}
	}
	_ = reader.raw.Close()
	return
}
