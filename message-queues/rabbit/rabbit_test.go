package rabbit_test

import (
	"fmt"
	amqp "github.com/rabbitmq/amqp091-go"
	"testing"
	"time"
)

func TestConsume(t *testing.T) {
	conn, dialErr := amqp.Dial(`amqp://`)
	if dialErr != nil {
		fmt.Println(dialErr)
		return
	}
	defer conn.Close()
	ch, chErr := conn.Channel()
	if chErr != nil {
		fmt.Println("chErr", chErr)
		return
	}
	delivery, consumeErr := ch.Consume("fns.test", "t1", false, false, false, false, nil)
	if consumeErr != nil {
		fmt.Println("consumeErr", consumeErr)
		return
	}
	for {
		msg, ok := <-delivery
		if !ok {
			break
		}
		fmt.Println("msg", string(msg.Body), msg.Type, msg.DeliveryMode)
		ackErr := msg.Ack(false)
		if ackErr != nil {
			fmt.Println("ackErr", ackErr)
		}
	}
}

func TestPublish(t *testing.T) {
	conn, dialErr := amqp.Dial(`amqp://admin:freedom1581@106.14.203.132:15672/`)
	if dialErr != nil {
		fmt.Println(dialErr)
		return
	}
	defer conn.Close()
	ch, chErr := conn.Channel()
	if chErr != nil {
		fmt.Println("chErr", chErr)
		return
	}
	_ = ch.Confirm(false)
	confirm, pErr := ch.PublishWithDeferredConfirm("fns.fanout", "", false, false, amqp.Publishing{
		ContentType: "text/plain",
		Body:        []byte(time.Now().String()),
	})
	if pErr != nil {
		fmt.Println("pErr", pErr)
		return
	}
	fmt.Println("p", confirm)
	if confirm != nil {
		confirm.Wait()
		fmt.Println(confirm.DeliveryTag)
	}
}
