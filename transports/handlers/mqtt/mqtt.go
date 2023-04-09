package mqtt

import (
	"context"
	"fmt"
	"github.com/aacfactory/errors"
	"github.com/aacfactory/fns-contrib/transports/handlers/websockets"
	"github.com/aacfactory/fns/commons/uid"
	"github.com/aacfactory/fns/service"
	"github.com/aacfactory/logs"
)

const (
	_name = "mqtt"
)

func MQTT() (handler websockets.SubProtocolHandler) {
	handler = &Handler{
		service: newService(),
	}
	return
}

type Handler struct {
	appId     string
	log       logs.Logger
	service   *Service
	discovery service.EndpointDiscovery
}

func (handler *Handler) Name() (name string) {
	name = _name
	return
}

func (handler *Handler) Build(options websockets.SubProtocolHandlerOptions) (err error) {
	handler.appId = options.AppId
	handler.log = options.Log
	handler.discovery = options.Discovery
	return
}

func (handler *Handler) Handle(ctx context.Context, wc websockets.Connection) {
	conn := NewConnection(wc)
	id, mountErr := handler.mount(ctx, conn)
	if mountErr != nil {
		_ = conn.Write(Failed(mountErr).Encode())
		conn.Close()
		return
	}
	for {
		p, readErr := conn.Read()
		if readErr != nil {
			// todo handle err
			break
		}
		// todo handle message
		fmt.Println(p)
		// how to handle sub and publish
	}
	_ = handler.unmount(ctx, id)
	return
}

func (handler *Handler) Service() (service service.Service) {
	service = handler.service
	return
}

func (handler *Handler) Close() (err error) {
	return
}

func (handler *Handler) mount(ctx context.Context, conn *Connection) (id string, err error) {
	id = handler.service.mount(conn)
	endpoint, has := handler.discovery.Get(ctx, _name)
	if !has {
		handler.service.unmount(id)
		err = errors.Warning("mqtt: mount connection failed").WithCause(errors.Warning("mqtt: service was not found").WithMeta("service", _name))
		return
	}
	fr := endpoint.Request(
		ctx,
		service.NewRequest(
			ctx,
			_name, mountFn,
			service.NewArgument(&mountParam{
				ConnectionId: id,
				AppId:        handler.appId,
			}),
			service.WithDeviceId(handler.appId),
			service.WithRequestId(uid.UID()),
			service.WithInternalRequest(),
		),
	)
	_, resultErr := fr.Get(ctx)
	if resultErr != nil {
		handler.service.unmount(id)
		err = errors.Warning("mqtt: mount connection failed").WithCause(resultErr)
		return
	}
	return
}

func (handler *Handler) unmount(ctx context.Context, connId string) (err error) {
	handler.service.unmount(connId)
	endpoint, has := handler.discovery.Get(ctx, _name)
	if !has {
		err = errors.Warning("mqtt: mount connection failed").WithCause(errors.Warning("mqtt: service was not found").WithMeta("service", _name))
		return
	}
	fr := endpoint.Request(
		ctx,
		service.NewRequest(
			ctx,
			_name, unmountFn,
			service.NewArgument(&unmountParam{
				ConnectionId: connId,
			}),
			service.WithDeviceId(handler.appId),
			service.WithRequestId(uid.UID()),
			service.WithInternalRequest(),
		),
	)
	_, _ = fr.Get(ctx)
	return
}
