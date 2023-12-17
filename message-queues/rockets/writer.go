package rockets

import "github.com/apache/rocketmq-client-go/v2"

type Writer struct {
	raw rocketmq.TransactionProducer
}
