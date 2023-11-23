package websockets

import (
	"bytes"
	"fmt"
	"github.com/aacfactory/errors"
	"github.com/aacfactory/fns/commons/bytex"
	"github.com/aacfactory/fns/context"
	"github.com/aacfactory/fns/runtime"
	"github.com/aacfactory/fns/services"
	"github.com/aacfactory/json"
)

var (
	_sendFnName = []byte("send")
)

func Send(ctx context.Context, id []byte, data interface{}) (err error) {
	message, messageErr := json.Marshal(data)
	if messageErr != nil {
		err = errors.Warning("websockets: send message to connection failed").WithCause(messageErr)
		return
	}
	err = proxySend(ctx, SendParam{
		ConnectionId: bytex.ToString(id),
		Message:      message,
	})
	return
}

func WithEndpointId(endpointId []byte) SendOption {
	return func(options *SendOptions) {
		options.EndpointId = endpointId
	}
}

type SendOption func(options *SendOptions)

type SendOptions struct {
	EndpointId []byte
}

func proxySend(ctx context.Context, param SendParam, options ...SendOption) (err error) {
	opt := SendOptions{}
	for _, option := range options {
		option(&opt)
	}
	eps := runtime.Endpoints(ctx)
	requestOptions := make([]services.RequestOption, 0, 1)
	if endpointId := opt.EndpointId; len(endpointId) > 0 {
		requestOptions = append(requestOptions, services.WithEndpointId(endpointId))
	}
	_, handleErr := eps.Request(ctx, _endpointName, _sendFnName, param, requestOptions...)
	if handleErr != nil {
		err = handleErr
		return
	}
	return
}

type SendParam struct {
	ConnectionId string          `json:"connectionId"`
	Message      json.RawMessage `json:"message"`
}

func send(ctx context.Context, param SendParam) (err error) {
	connId := bytex.FromString(param.ConnectionId)
	if len(connId) == 0 {
		err = errors.Warning("websockets: send message to connection failed").WithCause(fmt.Errorf("connection id is reqiured"))
		return
	}
	message := param.Message
	if len(message) == 0 {
		err = errors.Warning("websockets: send message to connection failed").WithCause(fmt.Errorf("message is reqiured"))
		return
	}
	registration, hasRegistration := LoadRegistration(ctx)
	if !hasRegistration {
		err = errors.Warning("websockets: send message to connection failed").WithCause(fmt.Errorf("there is no registration in context"))
		return
	}

	endpointId, has, getErr := registration.Get(ctx, connId)
	if getErr != nil {
		err = errors.Warning("websockets: send message to connection failed").WithCause(getErr)
		return
	}
	if !has {
		err = errors.Warning("websockets: send message to connection failed").WithCause(fmt.Errorf("connection was not found in registration"))
		return
	}
	appId := runtime.AppId(ctx)
	if bytes.Equal(appId, endpointId) {
		// same host
		conns, hasConns := LoadConnections(ctx)
		if !hasConns {
			err = errors.Warning("websockets: send message to connection failed").WithCause(fmt.Errorf("there is no connections in context"))
			return
		}
		conn, hasConn := conns.Get(connId)
		if !hasConn {
			err = errors.Warning("websockets: send message to connection failed").WithCause(fmt.Errorf("connection was not found"))
			return
		}
		writeErr := conn.WriteText(message)
		if writeErr != nil {
			err = errors.Warning("websockets: send message to connection failed").WithCause(writeErr)
			return
		}
		return
	}
	// dispatch
	err = proxySend(ctx, param, WithEndpointId(endpointId))
	return
}

func sendFn(ctx services.Request) (v interface{}, err error) {
	if !ctx.Param().Exist() {
		err = errors.Warning("websockets: send message to connection failed").WithCause(fmt.Errorf("param is required"))
		return
	}
	param := SendParam{}
	paramErr := ctx.Param().Scan(&param)
	if paramErr != nil {
		err = errors.Warning("websockets: send message to connection failed").WithCause(paramErr)
		return
	}
	err = send(ctx, param)
	return
}
