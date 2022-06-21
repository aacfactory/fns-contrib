package nats

import (
	"context"
	"fmt"
	"github.com/aacfactory/errors"
	"github.com/aacfactory/fns/commons/container/ring"
	"github.com/nats-io/nats.go"
	"strings"
)

type connection struct {
	key  string
	conn *nats.Conn
}

func (conn *connection) Key() (key string) {
	key = conn.key
	return
}

func newProducer(conn *nats.Conn, name string, config *ProducerConfig) (producer *Producer, err error) {
	subject := strings.TrimSpace(config.Subject)
	if subject == "" {
		err = errors.Warning(fmt.Sprintf("nats: new %s producer failed", name)).WithCause(fmt.Errorf("subject is required"))
		return
	}
	size := config.Size
	if size < 1 {
		size = 8
	}
	conns := ring.New()
	for i := 0; i < size; i++ {
		conns.Append(&connection{
			key:  fmt.Sprintf("%v", i),
			conn: conn,
		})
	}
	producer = &Producer{
		name:    name,
		subject: subject,
		conns:   conns,
	}
	return
}

type Producer struct {
	name    string
	subject string
	conns   *ring.Ring
}

func (producer *Producer) Publish(_ context.Context, msg []byte) (ok bool, err errors.CodeError) {
	conn := producer.conns.Next().(*connection)
	if conn.conn.IsClosed() {
		err = errors.ServiceError("nats: publish failed").WithCause(fmt.Errorf("conn is closed"))
		return
	}
	publishErr := conn.conn.Publish(producer.subject, msg)
	if publishErr != nil {
		err = errors.ServiceError("nats: publish failed").WithCause(publishErr)
		return
	}
	return
}

func (producer *Producer) Close() (err error) {
	return
}
