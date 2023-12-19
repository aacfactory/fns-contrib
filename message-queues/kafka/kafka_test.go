package kafka_test

import (
	"fmt"
	"github.com/aacfactory/fns-contrib/message-queues/kafka"
	kconfigs "github.com/aacfactory/fns-contrib/message-queues/kafka/configs"
	"github.com/aacfactory/fns/configs"
	"github.com/aacfactory/fns/context"
	"github.com/aacfactory/fns/tests"
	"strconv"
	"testing"
	"time"
)

func instance() (err error) {
	config := configs.New()
	config.AddService(
		"kafka",
		kconfigs.Config{
			Generic: kconfigs.Generic{
				Brokers: []string{"127.0.0.1:9092"},
			},
			Producers: kconfigs.ProducerConfig{
				Enable: true,
			},
			Consumers: map[string]kconfigs.ConsumerConfig{
				"r1": {
					Group:  "g1",
					Topics: []string{"test"},
				},
			},
		},
	)
	service := kafka.New(kafka.WithConsumeHandler("r1", handler), kafka.WithConsumeErrorHandler(errHandler))
	err = tests.Setup(service, tests.WithConfig(config))
	if err == nil {
		ctx := tests.TODO()
		go service.Listen(ctx)
	}
	return
}

func TestKafka_New(t *testing.T) {
	setupErr := instance()
	if setupErr != nil {
		t.Errorf("%+v", setupErr)
		return
	}
	defer tests.Teardown()
	ctx := tests.TODO()
	for i := 0; i < 10; i++ {
		pubErr := kafka.Publish(ctx, kafka.NewMessage("test", []byte(strconv.Itoa(i)), []byte(time.Now().Format(time.RFC3339))))
		if pubErr != nil {
			t.Errorf("%+v", pubErr)
			return
		}
	}
	time.Sleep(60 * time.Second)
}

func handler(ctx context.Context, value []byte, meta kafka.Meta) (err error) {
	fmt.Println(
		"topic:", meta.Topic,
		"key:", string(meta.Key),
		"offset:", meta.Offset,
		"part:", meta.Partition,
		"value:", string(value),
	)
	return
}

func errHandler(topic string, partition int32, cause error) {
	fmt.Println("err:", topic, partition, fmt.Sprintf("%+v", cause))
	return
}
