package kafka_test

import (
	"github.com/aacfactory/fns-contrib/message-queues/kafka"
	"github.com/aacfactory/fns/configs"
	"github.com/aacfactory/fns/context"
	"github.com/aacfactory/fns/tests"
	"strconv"
	"testing"
	"time"
)

func instance(t *testing.T) (err error) {
	config := configs.New()
	config.AddService(
		"kafka",
		kafka.Config{
			Brokers: []string{"127.0.0.1:9092"},
			Options: kafka.OptionsConfig{},
			Writer: map[string]kafka.WriterConfig{
				"test": {},
			},
			Reader: map[string]kafka.ReaderConfig{
				"reader": {
					GroupId:        "reader1",
					Topics:         []string{"test"},
					AutoCommit:     true,
					IsolationLevel: "uncommitted",
					MaxBytes:       10e6,
				},
			},
		},
	)
	service := kafka.New(kafka.WithReader("reader", &Reader{t: t}))
	err = tests.Setup(service, tests.WithConfig(config))
	if err == nil {
		ctx := tests.TODO()
		go service.Listen(ctx)
	}
	return
}

func TestKafka_New(t *testing.T) {
	setupErr := instance(t)
	if setupErr != nil {
		t.Errorf("%+v", setupErr)
		return
	}
	defer tests.Teardown()
	ctx := tests.TODO()
	for i := 0; i < 10; i++ {
		pubErr := kafka.Publish(ctx, "test", kafka.NewMessage([]byte(strconv.Itoa(i)), []byte(time.Now().Format(time.RFC3339))))
		if pubErr != nil {
			t.Errorf("%+v", pubErr)
			return
		}
	}
	time.Sleep(10 * time.Second)
}

type Reader struct {
	t *testing.T
}

func (r *Reader) Handle(ctx context.Context, message kafka.Message) {
	r.t.Log("consume:", message.Topic(), string(message.Key()), string(message.Body()), message.Time().String())
	_ = message.Commit(ctx)
	return
}

func TestNewReader(t *testing.T) {

}
