package websockets

import (
	"context"
	"fmt"
	"github.com/aacfactory/errors"
	"github.com/aacfactory/fns-contrib/transports/handlers/websockets/websocket"
	"github.com/aacfactory/fns/commons/uid"
	"github.com/aacfactory/fns/service"
	"github.com/aacfactory/json"
	"github.com/aacfactory/workers"
	"github.com/valyala/bytebufferpool"
	"io"
	"time"
	"unicode/utf8"
)

var (
	ErrRequestMessageIsTooLarge = fmt.Errorf("message is too large")
)

type Task struct {
	*workers.AbstractLongTask
	appId                 string
	deviceId              string
	deviceIp              string
	conn                  *websocket.Conn
	discovery             service.EndpointDiscovery
	service               *Service
	readTimeout           time.Duration
	writeTimeout          time.Duration
	maxRequestMessageSize int64
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
	defer func(ctx context.Context, t *Task, id string) {
		err := recover()
		if err == nil {
			return
		}
		fmt.Println(fmt.Sprintf("panic: %+v", err))
		_ = t.unmount(ctx, id)
		t.Close()
	}(ctx, t, id)
	for {
		fmt.Println("reading...")
		if aborted, abortedCause := t.Aborted(); aborted {
			cause := abortedCause.Error()
			if len(cause) > 123 {
				cause = cause[0:123]
			}
			_ = conn.WriteControl(websocket.CloseMessage, websocket.FormatCloseMessage(websocket.CloseAbnormalClosure, cause), time.Now().Add(2*time.Second))
			break
		}
		t.Touch(t.readTimeout)
		mt, reader, nextReadErr := conn.NextReader()
		if nextReadErr != nil {
			if nextReadErr != io.EOF {
				cause := nextReadErr.Error()
				if len(cause) > 123 {
					cause = cause[0:123]
				}
				_ = conn.WriteControl(websocket.CloseMessage, websocket.FormatCloseMessage(websocket.CloseAbnormalClosure, cause), time.Now().Add(2*time.Second))
			} else {
				_ = conn.WriteControl(websocket.CloseMessage, websocket.FormatCloseMessage(websocket.CloseNormalClosure, ""), time.Now().Add(2*time.Second))
			}
			break
		}
		if mt == websocket.BinaryMessage {
			_ = conn.WriteControl(websocket.CloseMessage, websocket.FormatCloseMessage(websocket.CloseInvalidFramePayloadData, "binary message was unsupported"), time.Now().Add(2*time.Second))
			break
		}
		message, readErr := t.read(reader)
		if readErr != nil {
			if readErr == ErrRequestMessageIsTooLarge {
				_ = conn.WriteControl(websocket.CloseMessage, websocket.FormatCloseMessage(websocket.CloseMessageTooBig, ErrRequestMessageIsTooLarge.Error()), time.Now().Add(2*time.Second))
			} else {
				cause := readErr.Error()
				if len(cause) > 123 {
					cause = cause[0:123]
				}
				_ = conn.WriteControl(websocket.CloseMessage, websocket.FormatCloseMessage(websocket.CloseAbnormalClosure, cause), time.Now().Add(2*time.Second))
			}
			break
		}
		t.Touch(t.writeTimeout)
		if utf8.Valid(message) {
			_ = conn.WriteControl(websocket.CloseMessage, websocket.FormatCloseMessage(websocket.CloseInvalidFramePayloadData, "message is not utf-8"), time.Now().Add(2*time.Second))
			break
		}

		request := Request{}
		decodeErr := json.Unmarshal(message, &request)
		if decodeErr != nil {
			_ = conn.WriteControl(websocket.CloseMessage, websocket.FormatCloseMessage(websocket.CloseInvalidFramePayloadData, "decode message failed"), time.Now().Add(2*time.Second))
			break
		}
		requestErr := request.Validate()
		if requestErr != nil {
			_ = conn.WriteControl(websocket.CloseMessage, websocket.FormatCloseMessage(websocket.CloseInvalidFramePayloadData, "message is invalid"), time.Now().Add(2*time.Second))
			break
		}
		ctx = context.WithValue(ctx, connectionId, id)
		endpoint, has := t.discovery.Get(ctx, request.Service)
		if !has {
			writeErr := conn.WriteMessage(websocket.TextMessage, Failed(errors.NotFound("websockets: service was not found").WithMeta("service", request.Service)).Encode())
			if writeErr != nil {
				break
			}
			continue
		}

		fr := endpoint.Request(
			ctx,
			service.NewRequest(
				ctx,
				request.Service, request.Fn,
				service.NewArgument(request.Payload),
				service.WithDeviceId(t.deviceId),
				service.WithDeviceIp(t.deviceIp),
				service.WithRequestHeader(request.Header),
				service.WithRequestId(uid.UID()),
			),
		)
		result, resultErr := fr.Get(ctx)
		if resultErr != nil {
			writeErr := conn.WriteMessage(websocket.TextMessage, Failed(resultErr).Encode())
			if writeErr != nil {
				break
			}
			continue
		}
		if !result.Exist() {
			writeErr := conn.WriteMessage(websocket.TextMessage, []byte{'n', 'u', 'l', 'l'})
			if writeErr != nil {
				break
			}
			continue
		}
		writeErr := conn.WriteMessage(websocket.TextMessage, Succeed(result).Encode())
		if writeErr != nil {
			break
		}
	}
	fmt.Println("closed.....")
	_ = conn.Close()
	_ = t.unmount(ctx, id)
	t.Close()
	return
}

func (t *Task) read(reader io.Reader) (p []byte, err error) {
	buf := bytebufferpool.Get()
	defer bytebufferpool.Put(buf)
	n := int64(0)
	for {
		b := make([]byte, 0, 512)
		nn, readErr := reader.Read(b)
		if readErr != nil {
			if readErr != io.EOF {
				err = readErr
				return
			}
			if nn > 0 {
				n += int64(nn)
				if n > t.maxRequestMessageSize {
					err = ErrRequestMessageIsTooLarge
					return
				}
				b = b[0:nn]
				_, _ = buf.Write(b)
				break
			}
			break
		}
		n += int64(nn)
		if n > t.maxRequestMessageSize {
			err = ErrRequestMessageIsTooLarge
			return
		}
		b = b[0:nn]
		_, _ = buf.Write(b)
		t.Touch(t.readTimeout)
	}
	p = buf.Bytes()
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
