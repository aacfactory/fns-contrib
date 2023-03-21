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
	conn      *websocket.Conn
	discovery service.EndpointDiscovery
	service   *Service
}

func (t *Task) Execute(ctx context.Context) {
	conn := t.conn
	id := t.service.mount(conn)
	for {
		mt, p, readErr := conn.ReadMessage()
		if readErr != nil {
			_ = conn.Close()
			t.service.unmount(id)
			break
		}
		if mt == websocket.CloseMessage {
			_ = conn.Close()
			t.service.unmount(id)
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
			t.service.unmount(id)
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
			failed := Failed(errors.Warning("websockets: decode request failed").WithCause(decodeErr))
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
