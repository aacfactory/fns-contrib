package kafka

import (
	"context"
	"github.com/segmentio/kafka-go"
)

type MessageCommitter interface {
	Commit(ctx context.Context, msg kafka.Message) (err error)
}

type NoopMessageCommitter struct{}

func (committer *NoopMessageCommitter) Commit(ctx context.Context, msg kafka.Message) (err error) {
	return
}

type ExplicitMessageCommitter struct {
	reader *kafka.Reader
}

func (committer *ExplicitMessageCommitter) Commit(ctx context.Context, msg kafka.Message) (err error) {
	err = committer.reader.CommitMessages(ctx, msg)
	return
}
