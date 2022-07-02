package kafka

import (
	"context"
	"fmt"
	"github.com/aacfactory/configures"
	"github.com/aacfactory/errors"
	"github.com/aacfactory/fns/service"
	"github.com/aacfactory/json"
	"github.com/aacfactory/logs"
	"github.com/segmentio/kafka-go"
	"strings"
)

const (
	committerContextKey = "_kafka_committer"
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

type ConsumerMessage interface {
	Key() (key []byte)
	Body() (body []byte)
	Partition() (no int)
	Offset() (offset int64)
	HighWaterMark() (v int64)
	Commit(ctx context.Context) (err error)
	Raw() (raw kafka.Message)
}

type DefaultConsumerMessage struct {
	raw kafka.Message
}

func (msg *DefaultConsumerMessage) Key() (key []byte) {
	key = msg.raw.Key
	return
}

func (msg *DefaultConsumerMessage) Body() (body []byte) {
	body = msg.raw.Value
	return
}

func (msg *DefaultConsumerMessage) Partition() (no int) {
	no = msg.raw.Partition
	return
}

func (msg *DefaultConsumerMessage) Offset() (offset int64) {
	offset = msg.raw.Offset
	return
}

func (msg *DefaultConsumerMessage) HighWaterMark() (v int64) {
	v = msg.raw.HighWaterMark
	return
}

func (msg *DefaultConsumerMessage) Commit(ctx context.Context) (err error) {
	rv := ctx.Value(committerContextKey)
	if rv == nil {
		err = errors.Warning("kafka: this context is not from kafka consumer")
		return
	}
	committer, ok := rv.(MessageCommitter)
	if !ok {
		err = errors.Warning("kafka: type kafka message reader in this context is not matched")
		return
	}
	commitErr := committer.Commit(ctx, msg.raw)
	if commitErr != nil {
		err = errors.Warning("kafka: commit message failed").WithCause(commitErr).WithMeta("messageKey", string(msg.Key())).WithMeta("messageOffset", fmt.Sprintf("%d", msg.Offset())).WithMeta("topic", msg.raw.Topic)
		return
	}
	return
}

func (msg *DefaultConsumerMessage) Raw() (raw kafka.Message) {
	raw = msg.raw
	return
}

type ConsumerHandlerOptions struct {
	Log    logs.Logger
	Config configures.Config
}

type ConsumerHandlerBuilder func(options ConsumerHandlerOptions) (handler ConsumerHandler, err error)

type ConsumerHandler interface {
	Handle(ctx context.Context, message ConsumerMessage)
}

var (
	consumerHandlers = map[string]ConsumerHandlerBuilder{
		"default":       defaultConsumerHandlerBuilder,
		"user_consumer": userConsumerHandlerBuilder,
	}
)

func RegisterConsumerHandler(name string, builder ConsumerHandlerBuilder) {
	name = strings.TrimSpace(name)
	if name == "" {
		panic(fmt.Sprintf("%+v", errors.Warning("kafka: register consumer handler failed, name is required")))
	}
	if builder == nil {
		panic(fmt.Sprintf("%+v", errors.Warning("kafka: register consumer handler failed, builder is required")))
	}
	_, has := consumerHandlers[name]
	if !has {
		panic(fmt.Sprintf("%+v", errors.Warning(fmt.Sprintf("kafka: register consumer handler failed, %s builder is registerd", name))))
	}
	consumerHandlers[name] = builder
}

func defaultConsumerHandlerBuilder(options ConsumerHandlerOptions) (handler ConsumerHandler, err error) {
	handler = &defaultConsumerHandler{
		log: options.Log,
	}
	return
}

type defaultConsumerHandler struct {
	log logs.Logger
}

func (handler *defaultConsumerHandler) Handle(ctx context.Context, message ConsumerMessage) {
	log := handler.log
	if log.DebugEnabled() {
		log.Debug().With("handler", "default").Message(fmt.Sprintf("kafka: consume message"))
	}
	body := message.Body()
	msg := &Message{}
	decodeErr := json.Unmarshal(body, msg)
	if decodeErr != nil {
		if log.ErrorEnabled() {
			log.Error().With("handler", "default").Cause(decodeErr).Message(fmt.Sprintf("kafka: consume message failed, decode failed"))
		}
		return
	}
	sn := msg.Service
	fn := msg.Fn
	if sn == "" || fn == "" {
		if log.ErrorEnabled() {
			log.Error().With("handler", "default").Cause(decodeErr).Message(fmt.Sprintf("kafka: consume message failed, decode failed"))
		}
		return
	}
	endpoint, hasEndpoint := service.GetEndpoint(ctx, sn)
	if !hasEndpoint {
		if log.WarnEnabled() {
			log.Warn().With("handler", "default").Message(fmt.Sprintf("kafka: consume message failed, %s service endpoint was not found", sn))
		}
		return
	}
	result := endpoint.Request(ctx, fn, service.NewArgument(msg.Argument))
	_, _, fnErr := result.Value(ctx)
	if fnErr != nil {
		if log.WarnEnabled() {
			log.Warn().With("handler", "default").Cause(fnErr).Message(fmt.Sprintf("kafka: consume message failed, %s service handle %s fn failed", sn, fn))
		}
		return
	}
	commitErr := message.Commit(ctx)
	if commitErr != nil {
		if log.WarnEnabled() {
			log.Warn().With("handler", "default").Cause(commitErr).Message(fmt.Sprintf("kafka: consume message failed, commit failed"))
		}
		return
	}
	return
}

func userConsumerHandlerBuilder(options ConsumerHandlerOptions) (handler ConsumerHandler, err error) {
	userId := ""
	_, getUserIdErr := options.Config.Get("userId", &userId)
	if getUserIdErr != nil {
		err = errors.Warning(fmt.Sprintf("kafka: used user_handler consumer handler but get userId from config failed")).WithCause(getUserIdErr)
		return
	}
	userId = strings.TrimSpace(userId)
	if userId == "" {
		err = errors.Warning(fmt.Sprintf("kafka: used user_handler consumer handler but userId is undefined in config"))
		return
	}
	handler = &userConsumerHandler{
		log:    options.Log,
		userId: userId,
	}
	return
}

type userConsumerHandler struct {
	log    logs.Logger
	userId string
}

func (handler *userConsumerHandler) Handle(ctx context.Context, message ConsumerMessage) {
	log := service.GetLog(ctx)
	if log.DebugEnabled() {
		log.Debug().With("handler", "user_handler").Message(fmt.Sprintf("kafka: consume message"))
	}
	body := message.Body()
	msg := &Message{}
	decodeErr := json.Unmarshal(body, msg)
	if decodeErr != nil {
		if log.ErrorEnabled() {
			log.Error().With("handler", "user_handler").Cause(decodeErr).Message(fmt.Sprintf("kafka: consume message failed, decode failed"))
		}
		return
	}
	sn := msg.Service
	fn := msg.Fn
	if sn == "" || fn == "" {
		if log.ErrorEnabled() {
			log.Error().With("handler", "user_handler").Cause(decodeErr).Message(fmt.Sprintf("kafka: consume message failed, decode failed"))
		}
		return
	}
	endpoint, hasEndpoint := service.GetEndpoint(ctx, sn)
	if !hasEndpoint {
		if log.WarnEnabled() {
			log.Warn().With("handler", "user_handler").Message(fmt.Sprintf("kafka: consume message failed, %s service endpoint was not found", sn))
		}
		return
	}
	request, requestErr := service.NewInternalRequest(sn, fn, service.NewArgument(msg.Argument))
	if requestErr != nil {
		if log.ErrorEnabled() {
			log.Error().With("handler", "user_handler").Cause(requestErr).Message(fmt.Sprintf("kafka: consume message failed, new internal request failed"))
		}
		return
	}
	request.SetUser(handler.userId, json.NewObject())
	result := endpoint.Request(ctx, fn, service.NewArgument(msg.Argument))
	_, _, fnErr := result.Value(ctx)
	if fnErr != nil {
		if log.WarnEnabled() {
			log.Warn().With("handler", "user_handler").Cause(fnErr).Message(fmt.Sprintf("kafka: consume message failed, %s service handle %s fn failed", sn, fn))
		}
		return
	}
	commitErr := message.Commit(ctx)
	if commitErr != nil {
		if log.WarnEnabled() {
			log.Warn().With("handler", "user_handler").Cause(commitErr).Message(fmt.Sprintf("kafka: consume message failed, commit failed"))
		}
		return
	}
	return
}

type Message struct {
	Service  string          `json:"service"`
	Fn       string          `json:"fn"`
	Argument json.RawMessage `json:"argument"`
}
