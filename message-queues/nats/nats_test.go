package nats_test

import (
	"fmt"
	"github.com/nats-io/nats.go"
	"testing"
	"time"
)

func TestProducer(t *testing.T) {
	nc, ncErr := nats.Connect("nats://:14222")
	if ncErr != nil {
		fmt.Println("nc", ncErr)
		return
	}
	pErr := nc.Publish("test", []byte(time.Now().String()))
	if pErr != nil {
		fmt.Println("pErr", pErr)
		return
	}
	_ = nc.Drain()
	nc.Close()
}

func TestConsumer11(t *testing.T) {
	nc, ncErr := nats.Connect("nats:admin//:14222")
	if ncErr != nil {
		fmt.Println("nc", ncErr)
		return
	}
	defer nc.Close()
	defer nc.Drain()
	sub, subErr := nc.QueueSubscribe("test", "1", func(msg *nats.Msg) {
		fmt.Println("q1.2:", string(msg.Data))
		_ = msg.Ack()
	})
	if subErr != nil {
		fmt.Println("nc", subErr)
		return
	}
	fmt.Println(sub.Queue, sub.Type())
	select {}
}

func TestConsumer12(t *testing.T) {
	nc, ncErr := nats.Connect("nats://:14222")
	if ncErr != nil {
		fmt.Println("nc", ncErr)
		return
	}
	defer nc.Close()
	defer nc.Drain()
	sub, subErr := nc.QueueSubscribe("test", "1", func(msg *nats.Msg) {
		fmt.Println("q1.2:", string(msg.Data))
		_ = msg.Ack()
	})
	if subErr != nil {
		fmt.Println("nc", subErr)
		return
	}
	fmt.Println(sub.Queue, sub.Type())
	select {}
}

func TestConsumer2(t *testing.T) {
	nc, ncErr := nats.Connect("nats://:14222")
	if ncErr != nil {
		fmt.Println("nc", ncErr)
		return
	}
	defer nc.Close()
	defer nc.Drain()
	sub, subErr := nc.QueueSubscribe("test", "2", func(msg *nats.Msg) {
		fmt.Println("q2:", string(msg.Data))
		_ = msg.Ack()
	})
	if subErr != nil {
		fmt.Println("nc", subErr)
		return
	}
	fmt.Println(sub.Queue, sub.Type())
	select {}
}
