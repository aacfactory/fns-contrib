package rabbit

import (
	"context"
	"fmt"
	"github.com/aacfactory/configuares"
	"github.com/aacfactory/errors"
	"github.com/aacfactory/fns/service"
	"github.com/aacfactory/json"
	"github.com/aacfactory/logs"
	amqp "github.com/rabbitmq/amqp091-go"
	"strings"
)

type ConsumerMessage interface {
	Id() (id string)
	ContentType() (contentType string)
	Body() (body []byte)
	Type() (typ string)
	Ack() (err error)
	Reject() (err error)
	Raw() (raw *amqp.Delivery)
}

type DefaultConsumerMessage struct {
	raw *amqp.Delivery
}

func (msg *DefaultConsumerMessage) Id() (id string) {
	id = msg.raw.MessageId
	return
}

func (msg *DefaultConsumerMessage) ContentType() (contentType string) {
	contentType = msg.raw.ContentType
	return
}

func (msg *DefaultConsumerMessage) Body() (body []byte) {
	body = msg.raw.Body
	return
}

func (msg *DefaultConsumerMessage) Type() (typ string) {
	typ = msg.raw.Type
	return
}

func (msg *DefaultConsumerMessage) Ack() (err error) {
	err = msg.raw.Ack(false)
	if err != nil {
		err = errors.ServiceError("rabbitmq: delegates an acknowledgement failed").WithCause(err)
	}
	return
}

func (msg *DefaultConsumerMessage) Reject() (err error) {
	err = msg.raw.Reject(true)
	if err != nil {
		err = errors.ServiceError("rabbitmq: delegates an reject failed").WithCause(err)
	}
	return
}

func (msg *DefaultConsumerMessage) Raw() (raw *amqp.Delivery) {
	raw = msg.raw
	return
}

type ConsumerHandlerOptions struct {
	Log    logs.Logger
	Config configuares.Config
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
		panic(fmt.Sprintf("%+v", errors.Warning("rabbitmq: register consumer handler failed, name is required")))
	}
	if builder == nil {
		panic(fmt.Sprintf("%+v", errors.Warning("rabbitmq: register consumer handler failed, builder is required")))
	}
	_, has := consumerHandlers[name]
	if !has {
		panic(fmt.Sprintf("%+v", errors.Warning(fmt.Sprintf("rabbitmq: register consumer handler failed, %s builder is registerd", name))))
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
		log.Debug().With("handler", "default").Message(fmt.Sprintf("rabbitmq: consume message[%s]", message.Id()))
	}
	body := message.Body()
	msg := &Message{}
	decodeErr := json.Unmarshal(body, msg)
	if decodeErr != nil {
		if log.ErrorEnabled() {
			log.Error().With("handler", "default").Cause(decodeErr).Message(fmt.Sprintf("rabbitmq: consume message[%s] failed, decode failed", message.Id()))
		}
		return
	}
	sn := msg.Service
	fn := msg.Fn
	if sn == "" || fn == "" {
		if log.ErrorEnabled() {
			log.Error().With("handler", "default").Cause(decodeErr).Message(fmt.Sprintf("rabbitmq: consume message[%s] failed, decode failed", message.Id()))
		}
		return
	}
	endpoint, hasEndpoint := service.GetEndpoint(ctx, sn)
	if !hasEndpoint {
		if log.WarnEnabled() {
			log.Warn().With("handler", "default").Message(fmt.Sprintf("rabbitmq: consume message[%s] failed, %s service endpoint was not found", message.Id(), sn))
		}
		return
	}
	result := endpoint.Request(ctx, fn, service.NewArgument(msg.Argument))
	_, _, fnErr := result.Value(ctx)
	if fnErr != nil {
		if log.WarnEnabled() {
			log.Warn().With("handler", "default").Cause(fnErr).Message(fmt.Sprintf("rabbitmq: consume message[%s] failed, %s service handle %s fn failed", message.Id(), sn, fn))
		}
		rejectErr := message.Reject()
		if rejectErr != nil {
			if log.WarnEnabled() {
				log.Warn().With("handler", "default").Cause(rejectErr).Message(fmt.Sprintf("rabbitmq: consume message[%s] failed, reject failed", message.Id()))
			}
			return
		}
		return
	}
	ackErr := message.Ack()
	if ackErr != nil {
		if log.WarnEnabled() {
			log.Warn().With("handler", "default").Cause(ackErr).Message(fmt.Sprintf("rabbitmq: consume message[%s] failed, ack failed", message.Id()))
		}
		return
	}
	return
}

func userConsumerHandlerBuilder(options ConsumerHandlerOptions) (handler ConsumerHandler, err error) {
	userId := ""
	_, getUserIdErr := options.Config.Get("userId", &userId)
	if getUserIdErr != nil {
		err = errors.Warning(fmt.Sprintf("rabbitmq: used user_handler consumer handler but get userId from config failed")).WithCause(getUserIdErr)
		return
	}
	userId = strings.TrimSpace(userId)
	if userId == "" {
		err = errors.Warning(fmt.Sprintf("rabbitmq: used user_handler consumer handler but userId is undefined in config"))
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
		log.Debug().With("handler", "default").Message(fmt.Sprintf("rabbitmq: consume message[%s]", message.Id()))
	}
	body := message.Body()
	msg := &Message{}
	decodeErr := json.Unmarshal(body, msg)
	if decodeErr != nil {
		if log.ErrorEnabled() {
			log.Error().With("handler", "default").Cause(decodeErr).Message(fmt.Sprintf("rabbitmq: consume message[%s] failed, decode failed", message.Id()))
		}
		return
	}
	sn := msg.Service
	fn := msg.Fn
	if sn == "" || fn == "" {
		if log.ErrorEnabled() {
			log.Error().With("handler", "default").Cause(decodeErr).Message(fmt.Sprintf("rabbitmq: consume message[%s] failed, decode failed", message.Id()))
		}
		return
	}
	endpoint, hasEndpoint := service.GetEndpoint(ctx, sn)
	if !hasEndpoint {
		if log.WarnEnabled() {
			log.Warn().With("handler", "default").Message(fmt.Sprintf("rabbitmq: consume message[%s] failed, %s service endpoint was not found", message.Id(), sn))
		}
		return
	}
	request, requestErr := service.NewInternalRequest(sn, fn, service.NewArgument(msg.Argument))
	if requestErr != nil {
		if log.ErrorEnabled() {
			log.Error().With("handler", "default").Cause(requestErr).Message(fmt.Sprintf("rabbitmq: consume message[%s] failed, new internal request failed", message.Id()))
		}
		return
	}
	request.SetUser(handler.userId, json.NewObject())
	result := endpoint.Request(ctx, fn, service.NewArgument(msg.Argument))
	_, _, fnErr := result.Value(ctx)
	if fnErr != nil {
		if log.WarnEnabled() {
			log.Warn().With("handler", "default").Cause(fnErr).Message(fmt.Sprintf("rabbitmq: consume message[%s] failed, %s service handle %s fn failed", message.Id(), sn, fn))
		}
		rejectErr := message.Reject()
		if rejectErr != nil {
			if log.WarnEnabled() {
				log.Warn().With("handler", "default").Cause(rejectErr).Message(fmt.Sprintf("rabbitmq: consume message[%s] failed, reject failed", message.Id()))
			}
			return
		}
		return
	}
	ackErr := message.Ack()
	if ackErr != nil {
		if log.WarnEnabled() {
			log.Warn().With("handler", "default").Cause(ackErr).Message(fmt.Sprintf("rabbitmq: consume message[%s] failed, ack failed", message.Id()))
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
