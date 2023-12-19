package kafka

import (
	"github.com/aacfactory/configures"
	"github.com/aacfactory/fns/context"
	"github.com/aacfactory/logs"
	"time"
)

type Meta struct {
	Topic           string
	Headers         Headers
	Key             []byte
	Partition       int64
	Offset          int64
	Time            time.Time
	ProducerEpoch   int16
	ProducerId      int64
	LeaderEpoch     int32
	TimestampType   TimestampType
	CompressionType CompressionType
	IsTransactional bool
	IsControl       bool
}

type ConsumerOptions struct {
	Log    logs.Logger
	Config configures.Config
}

type MessageHandler interface {
	Handle(ctx context.Context, value []byte, meta Meta) (err error)
}

type Consumer interface {
	Name() string
	Construct(ctx context.Context, options ConsumerOptions) (err error)
	MessageHandler
	Shutdown(ctx context.Context)
}

type GroupConsumer struct {
	handler MessageHandler
}
