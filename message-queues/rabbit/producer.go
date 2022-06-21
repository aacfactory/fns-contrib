package rabbit

import (
	"context"
	"fmt"
	"github.com/aacfactory/errors"
	"github.com/aacfactory/fns/commons/container/ring"
	"github.com/aacfactory/json"
	amqp "github.com/rabbitmq/amqp091-go"
	"strings"
)

type ProducerMessage interface {
	ContentType() (contentType string)
	Body() (body []byte)
}

type producerMessage struct {
	ContentType_ string          `json:"contentType"`
	Body_        json.RawMessage `json:"body"`
}

func (msg *producerMessage) ContentType() (contentType string) {
	contentType = msg.ContentType_
	return
}

func (msg *producerMessage) Body() (body []byte) {
	body = msg.Body_
	return
}

type channel struct {
	key string
	ch  *amqp.Channel
}

func (ch *channel) Key() (key string) {
	key = ch.key
	return
}

func newProducer(conn *amqp.Connection, name string, config *ProducerConfig) (producer *Producer, err error) {
	exchange := strings.TrimSpace(config.Exchange)
	if exchange == "" {
		err = errors.Warning(fmt.Sprintf("rabbitmq: new %s producer failed", name)).WithCause(fmt.Errorf("exchange is required"))
		return
	}
	size := config.Size
	if size < 1 {
		size = 8
	}
	channels := ring.New()
	for i := 0; i < size; i++ {
		ch, chErr := conn.Channel()
		if chErr != nil {
			err = errors.Warning(fmt.Sprintf("rabbitmq: new %s producer failed", name)).WithCause(chErr)
			return
		}
		if config.ConfirmMode {
			confirmModeErr := ch.Confirm(false)
			if confirmModeErr != nil {
				err = errors.Warning(fmt.Sprintf("rabbitmq: new %s producer failed", name)).WithCause(confirmModeErr)
				return
			}
		}
		channels.Append(&channel{
			key: fmt.Sprintf("%v", i),
			ch:  ch,
		})
	}
	producer = &Producer{
		name:        name,
		exchange:    exchange,
		confirmMode: config.ConfirmMode,
		key:         strings.TrimSpace(config.Key),
		mandatory:   config.Mandatory,
		immediate:   config.Immediate,
		channels:    channels,
	}
	return
}

type Producer struct {
	name        string
	exchange    string
	confirmMode bool
	key         string
	mandatory   bool
	immediate   bool
	channels    *ring.Ring
}

func (producer *Producer) Publish(_ context.Context, msg ProducerMessage) (ok bool, err errors.CodeError) {
	contentType := msg.ContentType()
	if contentType == "" {
		contentType = "text/plain"
	}
	body := msg.Body()
	if body == nil || len(body) == 0 {
		err = errors.ServiceError("rabbitmq: publish failed").WithCause(fmt.Errorf("message body is nil or empty"))
		return
	}

	ch := producer.channels.Next().(*channel)
	if ch.ch.IsClosed() {
		err = errors.ServiceError("rabbitmq: publish failed").WithCause(fmt.Errorf("channel is closed"))
		return
	}
	if producer.confirmMode {
		confirm, publishErr := ch.ch.PublishWithDeferredConfirm(producer.exchange, producer.exchange, producer.mandatory, producer.immediate, amqp.Publishing{
			ContentType: contentType,
			Body:        body,
		})
		if publishErr != nil {
			err = errors.ServiceError("rabbitmq: publish failed").WithCause(publishErr)
			return
		}
		if confirm != nil {
			ok = confirm.Wait()
		} else {
			ok = true
		}
	} else {
		publishErr := ch.ch.Publish(producer.exchange, producer.exchange, producer.mandatory, producer.immediate, amqp.Publishing{
			ContentType: contentType,
			Body:        body,
		})
		if publishErr != nil {
			err = errors.ServiceError("rabbitmq: publish failed").WithCause(publishErr)
			return
		}
		ok = true
	}
	return
}

func (producer *Producer) Close() (err error) {
	size := producer.channels.Size()
	for i := 0; i < size; i++ {
		ch := producer.channels.Next().(*channel)
		_ = ch.ch.Close()
	}
	return
}
