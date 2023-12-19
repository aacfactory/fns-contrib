package kafka

import (
	"github.com/aacfactory/fns/context"
	"github.com/aacfactory/logs"
)

type PartitionConsumerConfig struct {
}

type PartitionConsumer struct {
	log     logs.Logger
	name    string
	handler MessageHandler
}

func (consumer *PartitionConsumer) Name() string {
	return consumer.name
}

func (consumer *PartitionConsumer) Construct(ctx context.Context, options ConsumerOptions) (err error) {

	return
}

func (consumer *PartitionConsumer) Handle(ctx context.Context, value []byte, meta Meta) (err error) {

	return
}

func (consumer *PartitionConsumer) Shutdown(ctx context.Context) {

	return
}
