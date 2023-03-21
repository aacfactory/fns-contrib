package websockets

import (
	"context"
	"github.com/aacfactory/errors"
	"github.com/aacfactory/fns/commons/bytex"
	"github.com/aacfactory/fns/commons/uid"
	"github.com/aacfactory/fns/service"
	"github.com/aacfactory/json"
	"github.com/fasthttp/websocket"
	"time"
)

type Task struct {
	appId     string
	conn      *websocket.Conn
	discovery service.EndpointDiscovery
	service   *Service
}

func (t *Task) Execute(ctx context.Context) {
	conn := t.conn
	id, mountErr := t.mount(ctx, conn)
	if mountErr != nil {
		failed := Failed(mountErr)
		_ = conn.WriteControl(websocket.CloseMessage, failed.Encode(), time.Now().Add(2*time.Second))
		_ = conn.Close()
		return
	}
	for {
		mt, p, readErr := conn.ReadMessage()
		if readErr != nil {
			_ = conn.Close()
			_ = t.unmount(ctx, id)
			break
		}
		if mt == websocket.CloseMessage {
			_ = conn.Close()
			_ = t.unmount(ctx, id)
			break
		}
		if mt == websocket.PingMessage {
			_ = conn.WriteControl(websocket.PongMessage, bytex.FromString("pong"), time.Now().Add(2*time.Second))
			continue
		}
		if mt == websocket.PongMessage {
			break
		}
		if mt == websocket.BinaryMessage {
			failed := Failed(errors.Warning("websockets: binary message was unsupported"))
			_ = conn.WriteControl(websocket.CloseMessage, failed.Encode(), time.Now().Add(2*time.Second))
			_ = conn.Close()
			_ = t.unmount(ctx, id)
			break
		}
		request := Request{}
		decodeErr := json.Unmarshal(p, &request)
		if decodeErr != nil {
			failed := Failed(errors.Warning("websockets: decode request failed").WithCause(decodeErr))
			_ = conn.WriteMessage(websocket.TextMessage, failed.Encode())
			continue
		}
		requestErr := request.Validate()
		if requestErr != nil {
			failed := Failed(requestErr)
			_ = conn.WriteMessage(websocket.TextMessage, failed.Encode())
			continue
		}
		ctx = context.WithValue(ctx, connectionId, id)
		endpoint, has := t.discovery.Get(ctx, request.Service)
		if !has {
			failed := Failed(errors.NotFound("websockets: service was not found").WithMeta("service", request.Service))
			_ = conn.WriteMessage(websocket.TextMessage, failed.Encode())
			continue
		}
		fr := endpoint.Request(
			ctx,
			service.NewRequest(
				ctx,
				request.Service, request.Fn,
				service.NewArgument(request.Payload),
				service.WithDeviceId(request.DeviceId()),
				service.WithDeviceIp(request.DeviceIp()),
				service.WithHttpRequestHeader(request.Header),
				service.WithRequestId(uid.UID()),
			),
		)
		result, resultErr := fr.Get(ctx)
		if resultErr != nil {
			failed := Failed(resultErr)
			_ = conn.WriteMessage(websocket.TextMessage, failed.Encode())
			continue
		}
		if !result.Exist() {
			continue
		}
		succeed := Succeed(result)
		_ = conn.WriteMessage(websocket.TextMessage, succeed.Encode())
	}
	return
}

func (t *Task) mount(ctx context.Context, conn *websocket.Conn) (id string, err error) {
	id = t.service.mount(conn)
	endpoint, has := t.discovery.Get(ctx, handleName)
	if !has {
		t.service.unmount(id)
		err = errors.Warning("websockets: mount connection failed").WithCause(errors.Warning("websockets: service was not found").WithMeta("service", handleName))
		return
	}
	fr := endpoint.Request(
		ctx,
		service.NewRequest(
			ctx,
			handleName, mountFn,
			service.NewArgument(&mountParam{
				ConnectionId: id,
				AppId:        t.appId,
			}),
			service.WithDeviceId(t.appId),
			service.WithRequestId(uid.UID()),
			service.WithInternalRequest(),
		),
	)
	_, resultErr := fr.Get(ctx)
	if resultErr != nil {
		t.service.unmount(id)
		err = errors.Warning("websockets: mount connection failed").WithCause(resultErr)
		return
	}
	return
}

func (t *Task) unmount(ctx context.Context, id string) (err error) {
	t.service.unmount(id)
	endpoint, has := t.discovery.Get(ctx, handleName)
	if !has {
		err = errors.Warning("websockets: mount connection failed").WithCause(errors.Warning("websockets: service was not found").WithMeta("service", handleName))
		return
	}
	fr := endpoint.Request(
		ctx,
		service.NewRequest(
			ctx,
			handleName, unmountFn,
			service.NewArgument(&unmountParam{
				ConnectionId: id,
			}),
			service.WithDeviceId(t.appId),
			service.WithRequestId(uid.UID()),
			service.WithInternalRequest(),
		),
	)
	_, _ = fr.Get(ctx)
	return
}
