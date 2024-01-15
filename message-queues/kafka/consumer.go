package kafka

import (
	"bytes"
	"compress/gzip"
	sc "context"
	"fmt"
	"github.com/aacfactory/errors"
	"github.com/aacfactory/fns-contrib/message-queues/kafka/configs"
	"github.com/aacfactory/fns/context"
	"github.com/aacfactory/logs"
	"github.com/klauspost/compress/snappy"
	"github.com/klauspost/compress/zstd"
	"github.com/pierrec/lz4/v4"
	"github.com/twmb/franz-go/pkg/kgo"
	"io"
	"sync"
	"time"
)

const (
	UnknownTimestamp = iota - 1
	DefaultTimestamp
	AlternativeTimestamp
)

type TimestampType int8

type Meta struct {
	Topic           string
	Headers         Headers
	Key             []byte
	Partition       int32
	Offset          int64
	Time            time.Time
	ProducerEpoch   int16
	ProducerId      int64
	LeaderEpoch     int32
	TimestampType   TimestampType
	IsTransactional bool
	IsControl       bool
}

type ConsumerOptions struct {
	Log logs.Logger

	Config configs.ConsumerConfig
}

// ConsumeHandler
// value is uncompressed
type ConsumeHandler func(ctx context.Context, value []byte, meta Meta) (err error)

type ConsumeErrorHandler func(topic string, partition int32, cause error)

type Consumer interface {
	Listen(ctx context.Context) (err error)
	Shutdown(ctx context.Context)
}

type TopicPartitionKey struct {
	Topic     string
	Partition int32
}

func NewGroupConsumer(log logs.Logger, maxPollRecords int, partitionBuffer int, opts []kgo.Opt, handler ConsumeHandler, errorHandler ConsumeErrorHandler) (v Consumer, err error) {
	if maxPollRecords < 1 {
		maxPollRecords = 1024
	}
	if partitionBuffer < 1 {
		partitionBuffer = 8
	}

	consumer := &GroupConsumer{
		name:            "",
		log:             log,
		opts:            nil,
		client:          nil,
		quit:            make(chan struct{}),
		maxPollRecords:  maxPollRecords,
		partitionBuffer: partitionBuffer,
		handler:         handler,
		errorHandler:    errorHandler,
		partitions:      make(map[TopicPartitionKey]*GroupPartitionConsumer),
	}
	opts = append(opts, kgo.OnPartitionsAssigned(consumer.assigned))
	opts = append(opts, kgo.OnPartitionsRevoked(consumer.assigned))
	opts = append(opts, kgo.OnPartitionsLost(consumer.assigned))
	consumer.opts = opts

	v = consumer
	return
}

type GroupConsumer struct {
	ctx             context.Context
	name            string
	log             logs.Logger
	opts            []kgo.Opt
	client          *kgo.Client
	quit            chan struct{}
	maxPollRecords  int
	partitionBuffer int
	handler         ConsumeHandler
	errorHandler    ConsumeErrorHandler
	partitions      map[TopicPartitionKey]*GroupPartitionConsumer
}

func (consumer *GroupConsumer) Listen(ctx context.Context) (err error) {
	client, clientErr := kgo.NewClient(consumer.opts...)
	if clientErr != nil {
		err = errors.Warning("kafka: group consumer listen failed").WithMeta("group", consumer.name).WithCause(clientErr)
		return
	}
	consumer.ctx = ctx
	consumer.client = client
	stopped := false
	for {
		select {
		case <-ctx.Done():
			stopped = true
			break
		case <-consumer.quit:
			stopped = true
			break
		default:
			fetches := consumer.client.PollRecords(ctx, consumer.maxPollRecords)
			fetches.EachPartition(func(p kgo.FetchTopicPartition) {
				if p.Err != nil {
					if consumer.errorHandler != nil {
						consumer.errorHandler(p.Topic, p.Partition, p.Err)
					}
					return
				}
				key := TopicPartitionKey{
					Topic:     p.Topic,
					Partition: p.Partition,
				}
				consumer.partitions[key].records <- p.Records
			})
			consumer.client.AllowRebalance()
		}
		if stopped {
			break
		}
	}
	return
}

func (consumer *GroupConsumer) Shutdown(ctx context.Context) {

	return
}

func (consumer *GroupConsumer) assigned(_ sc.Context, client *kgo.Client, assigned map[string][]int32) {
	for topic, partitions := range assigned {
		for _, partition := range partitions {
			pc := &GroupPartitionConsumer{
				client:     client,
				topic:      topic,
				partition:  partition,
				quit:       make(chan struct{}),
				done:       make(chan struct{}),
				records:    make(chan []*kgo.Record, consumer.partitionBuffer),
				handler:    consumer.handler,
				errHandler: consumer.errorHandler,
			}
			key := TopicPartitionKey{
				Topic:     topic,
				Partition: partition,
			}
			consumer.partitions[key] = pc
			go pc.Consume(consumer.ctx)
		}
	}
}

func (consumer *GroupConsumer) lost(_ context.Context, _ *kgo.Client, lost map[string][]int32) {
	var wg sync.WaitGroup
	defer wg.Wait()
	for topic, partitions := range lost {
		for _, partition := range partitions {
			key := TopicPartitionKey{
				Topic:     topic,
				Partition: partition,
			}
			pc := consumer.partitions[key]
			delete(consumer.partitions, key)
			close(pc.quit)
			wg.Add(1)
			go func() { <-pc.done; wg.Done() }()
		}
	}
}

type GroupPartitionConsumer struct {
	client     *kgo.Client
	topic      string
	partition  int32
	quit       chan struct{}
	done       chan struct{}
	records    chan []*kgo.Record
	handler    ConsumeHandler
	errHandler ConsumeErrorHandler
}

func (consumer *GroupPartitionConsumer) Consume(ctx context.Context) {
	defer close(consumer.done)
	for {
		select {
		case <-consumer.quit:
			return
		case records := <-consumer.records:
			for _, record := range records {
				meta := Meta{
					Topic:           record.Topic,
					Headers:         convertHeaders(record.Headers),
					Key:             record.Key,
					Partition:       record.Partition,
					Offset:          record.Offset,
					Time:            record.Timestamp,
					ProducerEpoch:   record.ProducerEpoch,
					ProducerId:      record.ProducerID,
					LeaderEpoch:     record.LeaderEpoch,
					TimestampType:   TimestampType(record.Attrs.TimestampType()),
					IsTransactional: record.Attrs.IsTransactional(),
					IsControl:       record.Attrs.IsControl(),
				}
				var value []byte
				switch record.Attrs.CompressionType() {
				case 1:
					// gzip
					reader, readerErr := gzip.NewReader(bytes.NewReader(record.Value))
					if readerErr != nil {
						if consumer.errHandler != nil {
							readerErr = errors.Warning("kafka: read record failed").WithCause(readerErr).WithMeta("compression", "gzip").WithMeta("value", string(value)).WithMeta("offset", fmt.Sprintf("%d", record.Offset))
							consumer.errHandler(record.Topic, record.Partition, readerErr)
						}
						continue
					}
					p, readErr := io.ReadAll(reader)
					if readErr != nil {
						if consumer.errHandler != nil {
							readErr = errors.Warning("kafka: read record failed").WithCause(readErr).WithMeta("compression", "gzip").WithMeta("value", string(value)).WithMeta("offset", fmt.Sprintf("%d", record.Offset))
							consumer.errHandler(record.Topic, record.Partition, readErr)
						}
						continue
					}
					value = p
					break
				case 2:
					// snappy
					n, nErr := snappy.DecodedLen(record.Value)
					if nErr != nil {
						if consumer.errHandler != nil {
							nErr = errors.Warning("kafka: read record failed").WithCause(nErr).WithMeta("compression", "snappy").WithMeta("value", string(value)).WithMeta("offset", fmt.Sprintf("%d", record.Offset))
							consumer.errHandler(record.Topic, record.Partition, nErr)
						}
						continue
					}
					value = make([]byte, n)
					value, nErr = snappy.Decode(value, record.Value)
					if nErr != nil {
						if consumer.errHandler != nil {
							nErr = errors.Warning("kafka: read record failed").WithCause(nErr).WithMeta("compression", "snappy").WithMeta("value", string(value)).WithMeta("offset", fmt.Sprintf("%d", record.Offset))
							consumer.errHandler(record.Topic, record.Partition, nErr)
						}
						continue
					}
					break
				case 3:
					// lz4
					reader := lz4.NewReader(bytes.NewReader(record.Value))
					p, readErr := io.ReadAll(reader)
					if readErr != nil {
						if consumer.errHandler != nil {
							readErr = errors.Warning("kafka: read record failed").WithCause(readErr).WithMeta("compression", "lz4").WithMeta("value", string(value)).WithMeta("offset", fmt.Sprintf("%d", record.Offset))
							consumer.errHandler(record.Topic, record.Partition, readErr)
						}
						continue
					}
					value = p
					break
				case 4:
					// zstd
					reader, readerErr := zstd.NewReader(bytes.NewReader(record.Value))
					if readerErr != nil {
						if consumer.errHandler != nil {
							readerErr = errors.Warning("kafka: read record failed").WithCause(readerErr).WithMeta("compression", "zstd").WithMeta("value", string(value)).WithMeta("offset", fmt.Sprintf("%d", record.Offset))
							consumer.errHandler(record.Topic, record.Partition, readerErr)
						}
						continue
					}
					p, readErr := io.ReadAll(reader)
					if readErr != nil {
						if consumer.errHandler != nil {
							readErr = errors.Warning("kafka: read record failed").WithCause(readErr).WithMeta("compression", "zstd").WithMeta("value", string(value)).WithMeta("offset", fmt.Sprintf("%d", record.Offset))
							consumer.errHandler(record.Topic, record.Partition, readErr)
						}
						continue
					}
					value = p
					break
				default:
					value = record.Value
					break
				}
				handleErr := consumer.handler(ctx, value, meta)
				if handleErr != nil {
					if consumer.errHandler != nil {
						handleErr = errors.Warning("kafka: handle failed").WithCause(handleErr).WithMeta("value", string(value)).WithMeta("offset", fmt.Sprintf("%d", record.Offset))
						consumer.errHandler(record.Topic, record.Partition, handleErr)
					}
				}
			}
			cmtErr := consumer.client.CommitRecords(sc.Background(), records...)
			if cmtErr != nil {
				if consumer.errHandler != nil {
					cmtErr = errors.Warning("kafka: commit failed").WithCause(cmtErr)
					consumer.errHandler(consumer.topic, consumer.partition, cmtErr)
				}
			}
		}
	}
}
