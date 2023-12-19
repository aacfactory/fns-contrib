package kafka

import (
	"fmt"
	"github.com/aacfactory/errors"
	"github.com/aacfactory/fns/context"
	"github.com/aacfactory/logs"
	"github.com/twmb/franz-go/pkg/kgo"
	"sync/atomic"
)

func NewProducer(log logs.Logger, num int, opts []kgo.Opt) (v *Producer, err error) {
	if num < 1 {
		num = 1
	}
	clients := make([]*kgo.Client, num)
	for i := 0; i < num; i++ {
		client, clientErr := kgo.NewClient(opts...)
		if clientErr != nil {
			err = errors.Warning("kafka: new producer failed").WithCause(clientErr)
			return
		}
		clients[i] = client
	}

	v = &Producer{
		log:     log,
		stopped: atomic.Bool{},
		clients: clients,
		idx:     atomic.Uint64{},
		size:    uint64(num),
	}
	return
}

type Producer struct {
	log     logs.Logger
	stopped atomic.Bool
	clients []*kgo.Client
	idx     atomic.Uint64
	size    uint64
}

func (producer *Producer) Publish(ctx context.Context, messages []ProducerMessage, async bool) (err error) {
	records := make([]*kgo.Record, 0, 1)
	for _, message := range messages {
		r := kgo.KeySliceRecord(message.Key, message.Body)
		r.Topic = message.Topic
		if len(message.Headers) > 0 {
			r.Headers = message.Headers.ConvertToKafkaHeaders()
		}
		records = append(records, r)
	}
	if len(records) == 0 {
		return
	}
	if async {
		for _, record := range records {
			if producer.stopped.Load() {
				err = errors.Warning("kafka: publish failed").WithCause(fmt.Errorf("stopped"))
				return
			}
			producer.Client().Produce(ctx, record, nil)
		}
	} else {
		if producer.stopped.Load() {
			err = errors.Warning("kafka: publish failed").WithCause(fmt.Errorf("stopped"))
			return
		}
		err = producer.Client().ProduceSync(ctx, records...).FirstErr()
		if err != nil {
			err = errors.Warning("kafka: publish failed").WithCause(err)
			return
		}
	}
	return
}

func (producer *Producer) Client() (v *kgo.Client) {
	n := producer.idx.Add(1)
	pos := n % producer.size
	v = producer.clients[pos]
	return
}

func (producer *Producer) Shutdown(_ context.Context) {
	producer.stopped.Store(true)
	for _, client := range producer.clients {
		client.Close()
	}
	return
}
