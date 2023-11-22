package websockets

import (
	"bytes"
	"fmt"
	"github.com/aacfactory/configures"
	"github.com/aacfactory/errors"
	"github.com/aacfactory/fns-contrib/transports/handlers/websockets/websocket"
	"github.com/aacfactory/fns/commons/bytex"
	"github.com/aacfactory/fns/commons/uid"
	"github.com/aacfactory/fns/context"
	"github.com/aacfactory/fns/runtime"
	"github.com/aacfactory/fns/service"
	"github.com/aacfactory/fns/services"
	"github.com/aacfactory/fns/transports"
	"github.com/aacfactory/json"
	"github.com/aacfactory/logs"
	"io"
	"net/http"
	"sync/atomic"
	"time"
	"unicode/utf8"
)

const (
	handleName = "websockets"
)

var (
	ErrTooMayConnections = errors.New(http.StatusTooManyRequests, "***TOO MANY CONNECTIONS***", "fns: too may connections, try again later.")
)

func Websocket(subs ...SubProtocolHandler) (handler transports.MuxHandler) {
	wh := &websocketHandler{
		log:     nil,
		service: newService(),
		subs:    make(map[string]SubProtocolHandler),
	}
	if subs != nil && len(subs) > 0 {
		for _, sub := range subs {
			if sub == nil {
				continue
			}
			name := sub.Name()
			if _, has := wh.subs[name]; has {
				panic(fmt.Errorf("%+v", errors.Warning("websockets: sub protocol handler was duplicated").WithMeta("name", name)))
				return
			}
			wh.subs[name] = sub
		}
	}
	handler = wh
	return
}

type websocketHandler struct {
	log                   logs.Logger
	upgrader              *websocket.Upgrader
	service               *Service
	subs                  map[string]SubProtocolHandler
	readTimeout           time.Duration
	writeTimeout          time.Duration
	maxRequestMessageSize int64
	maxConnections        int64
	connections           *atomic.Int64
	originCheckFunc       func(r *transports.Request) bool
}

func (handler *websocketHandler) Name() (name string) {
	name = handleName
	return
}

func (handler *websocketHandler) Construct(options transports.MuxHandlerOptions) (err error) {
	handler.log = options.Log

	config := Config{}
	configErr := options.Config.As(&config)
	if configErr != nil {
		err = errors.Warning("websocket: construct failed").WithCause(configErr)
		return
	}
	handshakeTimeout := time.Duration(0)
	if config.HandshakeTimeout != "" {
		handshakeTimeout, err = time.ParseDuration(config.HandshakeTimeout)
		if err != nil {
			err = errors.Warning("websocket: construct failed").WithCause(errors.Warning("handshakeTimeout format must be time.Duration"))
			return
		}
	}
	readTimeout := time.Duration(0)
	if config.ReadTimeout != "" {
		readTimeout, err = time.ParseDuration(config.ReadTimeout)
		if err != nil {
			err = errors.Warning("websocket: build failed").WithCause(errors.Warning("readTimeout format must be time.Duration"))
			return
		}
	} else {
		readTimeout = 10 * time.Second
	}
	handler.readTimeout = readTimeout
	readBufferSize := uint64(0)
	if config.ReadBufferSize != "" {
		readBufferSize, err = bytex.ParseBytes(config.ReadBufferSize)
		if err != nil {
			err = errors.Warning("websocket: build failed").WithCause(errors.Warning("readBufferSize format must be byte"))
			return
		}
	}
	writeTimeout := time.Duration(0)
	if config.WriteTimeout != "" {
		writeTimeout, err = time.ParseDuration(config.WriteTimeout)
		if err != nil {
			err = errors.Warning("websocket: build failed").WithCause(errors.Warning("writeTimeout format must be time.Duration"))
			return
		}
	} else {
		writeTimeout = 60 * time.Second
	}
	handler.writeTimeout = writeTimeout
	writeBufferSize := uint64(0)
	if config.WriteBufferSize != "" {
		writeBufferSize, err = bytex.ParseBytes(config.WriteBufferSize)
		if err != nil {
			err = errors.Warning("websocket: build failed").WithCause(errors.Warning("writeBufferSize format must be byte"))
			return
		}
	}
	maxRequestMessageSize := uint64(0)
	if config.MaxRequestMessageSize != "" {
		maxRequestMessageSize, err = bytex.ParseBytes(config.MaxRequestMessageSize)
		if err != nil {
			err = errors.Warning("websocket: build failed").WithCause(errors.Warning("maxRequestMessageSize format must be byte"))
			return
		}
	} else {
		maxRequestMessageSize = uint64(4096)
	}
	handler.maxRequestMessageSize = int64(maxRequestMessageSize)

	originCheckFn, originCheckFnErr := config.OriginCheckPolicy.Build()
	if originCheckFnErr != nil {
		err = errors.Warning("websocket: build failed").WithCause(originCheckFnErr)
		return
	}

	handler.upgrader = &websocket.Upgrader{
		HandshakeTimeout: handshakeTimeout,
		ReadBufferSize:   int(readBufferSize),
		WriteBufferSize:  int(writeBufferSize),
		WriteBufferPool:  nil,
		Subprotocols:     nil,
		Error: func(w transports.ResponseWriter, r transports.Request, status int, reason error) {
			w.Failed(errors.Warning("websocket: handle failed").WithCause(reason))
			return
		},
		CheckOrigin:       originCheckFn,
		EnableCompression: config.EnableCompression,
	}

	if handler.subs != nil && len(handler.subs) > 0 {
		for name, sub := range handler.subs {
			subConfig, hasSubConfig := options.Config.Node(name)
			if !hasSubConfig {
				subConfig, _ = configures.NewJsonConfig([]byte{'{', '}'})
			}
			subOptions := SubProtocolHandlerOptions{
				Log:                   options.Log.With("protocol", name),
				Config:                subConfig,
				ReadTimeout:           readTimeout,
				WriteTimeout:          writeTimeout,
				MaxRequestMessageSize: handler.maxRequestMessageSize,
			}
			subErr := sub.Build(subOptions)
			if subErr != nil {
				err = errors.Warning("websocket: build failed").WithCause(subErr).WithMeta("sub", name)
				return
			}
		}
	}

	maxConnections := config.MaxConnections
	if maxConnections < 1 {
		maxConnections = 10240
	}
	handler.maxConnections = int64(maxConnections)
	handler.connections = new(atomic.Int64)
	return
}

func (handler *websocketHandler) Match(_ context.Context, method []byte, _ []byte, header transports.Header) (ok bool) {
	ok = bytes.Equal(method, transports.MethodGet) && websocket.IsWebSocketUpgrade(header)
	return
}

func (handler *websocketHandler) Handle(w transports.ResponseWriter, r transports.Request) {
	handler.connections.Add(1)
	if handler.connections.Load() > handler.maxConnections {
		handler.connections.Add(-1)
		w.Failed(ErrTooMayConnections)
		return
	}
	upgradeErr := handler.upgrader.Upgrade(w, r, handler.handleConn)
	if upgradeErr != nil {
		handler.connections.Add(-1)
		w.Failed(errors.Warning("websocket: upgrade failed").WithCause(upgradeErr))
		return
	}
	return
}

func (handler *websocketHandler) handleConn(ctx context.Context, conn *websocket.Conn, header transports.Header) {
	defer handler.connections.Add(-1)
	conn.SetPingHandler(nil)
	conn.SetPongHandler(func(appData string) error {
		return conn.WriteControl(websocket.PongMessage, bytex.FromString("pong"), time.Now().Add(2*time.Second))
	})
	protocol := header.Get([]byte("Sec-Websocket-Protocol"))
	if len(protocol) > 0 {
		sub, has := handler.subs[string(protocol)]
		if !has {
			_ = conn.WriteControl(websocket.CloseMessage, websocket.FormatCloseMessage(websocket.CloseProtocolError, "sub protocol is unsupported"), time.Now().Add(2*time.Second))
			_ = conn.Close()
			return
		}
		sub.Handle(ctx, &WebsocketConnection{
			Conn:     conn,
			header:   header,
			deviceId: string(header.Get([]byte("X-Fns-Device-Id"))),
			deviceIp: string(header.Get([]byte("X-Fns-Device-Ip"))),
		})
		return
	}
	handler.handle(ctx, conn, header)
	return
}

func (handler *websocketHandler) handle(ctx context.Context, conn *websocket.Conn, header transports.Header) {
	deviceId := header.Get(transports.DeviceIdHeaderName)
	deviceIp := header.Get(transports.DeviceIpHeaderName)
	id, mountErr := handler.mount(ctx, conn, string(deviceId))
	if mountErr != nil {
		failed := Failed(mountErr)
		_ = conn.WriteControl(websocket.CloseMessage, failed.Encode(), time.Now().Add(2*time.Second))
		_ = conn.Close()
		return
	}
	defer func(ctx context.Context, handler *websocketHandler, id string, deviceId string) {
		recovered := recover()
		if recovered == nil {
			return
		}
		err, isErr := recovered.(error)
		if isErr {
			if handler.log.WarnEnabled() {
				handler.log.Warn().Cause(errors.Map(err)).Message("websockets: panic at handling")
			}
		}
		_ = handler.unmount(ctx, id, deviceId)
	}(ctx, handler, id, string(deviceId))
	for {
		// read
		mt, reader, nextReadErr := conn.NextReader()
		if nextReadErr != nil {
			switch nextReadErr.(type) {
			case *websocket.CloseError:
				break
			default:
				if nextReadErr != io.EOF {
					cause := nextReadErr.Error()
					if len(cause) > 123 {
						cause = cause[0:123]
					}
					_ = conn.WriteControl(websocket.CloseMessage, websocket.FormatCloseMessage(websocket.CloseAbnormalClosure, cause), time.Now().Add(2*time.Second))
				} else {
					_ = conn.WriteControl(websocket.CloseMessage, websocket.FormatCloseMessage(websocket.CloseNormalClosure, ""), time.Now().Add(2*time.Second))
				}
			}
			break
		}
		if mt == websocket.BinaryMessage {
			_ = conn.WriteControl(websocket.CloseMessage, websocket.FormatCloseMessage(websocket.CloseInvalidFramePayloadData, "binary message was unsupported"), time.Now().Add(2*time.Second))
			break
		}
		message, readErr := readMessage(reader, handler.maxRequestMessageSize)
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
		if !utf8.Valid(message) {
			_ = conn.WriteControl(websocket.CloseMessage, websocket.FormatCloseMessage(websocket.CloseInvalidFramePayloadData, "message is not utf-8"), time.Now().Add(2*time.Second))
			break
		}
		// parse request
		request := Request{}
		decodeErr := json.Unmarshal(message, &request)
		if decodeErr != nil {
			_ = conn.WriteControl(websocket.CloseMessage, websocket.FormatCloseMessage(websocket.CloseInvalidFramePayloadData, "decode message failed"), time.Now().Add(2*time.Second))
			break
		}
		validateErr := request.Validate()
		if validateErr != nil {
			_ = conn.WriteControl(websocket.CloseMessage, websocket.FormatCloseMessage(websocket.CloseInvalidFramePayloadData, "message is invalid"), time.Now().Add(2*time.Second))
			break
		}
		rvs, rvsErr := request.Versions()
		if rvsErr != nil {
			_ = conn.WriteControl(websocket.CloseMessage, websocket.FormatCloseMessage(websocket.CloseInvalidFramePayloadData, "parse X-Fns-Request-Version failed"), time.Now().Add(2*time.Second))
			break
		}
		ctx.SetLocalValue(connectionId, id)
		eps := runtime.Endpoints(ctx)
		response, handleErr := eps.Request(
			ctx,
			bytex.FromString(request.Service), bytex.FromString(request.Fn), request.Payload,
			services.WithDeviceId(deviceId), services.WithDeviceIp(deviceIp),
			services.WithRequestVersions(rvs),
			services.WithRequestId(uid.Bytes()),
		)
		if handleErr != nil {
			writeErr := conn.WriteMessage(websocket.TextMessage, Failed(handleErr).Encode())
			if writeErr != nil {
				break
			}
			continue
		}
		if !response.Exist() {
			writeErr := conn.WriteMessage(websocket.TextMessage, []byte{'n', 'u', 'l', 'l'})
			if writeErr != nil {
				break
			}
			continue
		}
		writeErr := conn.WriteMessage(websocket.TextMessage, Succeed(response).Encode())
		if writeErr != nil {
			break
		}
	}
	_ = handler.unmount(ctx, id, string(deviceId))
}

func (handler *websocketHandler) mount(ctx context.Context, conn *websocket.Conn, deviceId string) (id string, err error) {
	id = handler.service.mount(conn)

	endpoint, has := handler.discovery.Get(ctx, handleName)
	if !has {
		handler.service.unmount(id)
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
				AppId:        handler.appId,
			}),
			service.WithDeviceId(deviceId),
			service.WithRequestId(uid.UID()),
			service.WithInternalRequest(),
		),
	)
	_, resultErr := fr.Get(ctx)
	if resultErr != nil {
		handler.service.unmount(id)
		err = errors.Warning("websockets: mount connection failed").WithCause(resultErr)
		return
	}
	return
}

func (handler *websocketHandler) unmount(ctx context.Context, id string, deviceId string) (err error) {
	handler.service.unmount(id)
	endpoint, has := handler.discovery.Get(ctx, handleName)
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
			service.WithDeviceId(deviceId),
			service.WithRequestId(uid.UID()),
			service.WithInternalRequest(),
		),
	)
	_, _ = fr.Get(ctx)
	return
}

func (handler *websocketHandler) Close() (err error) {
	errs := make(errors.Errors, 0, 1)
	if handler.subs != nil && len(handler.subs) > 0 {
		for name, sub := range handler.subs {
			subErr := sub.Close()
			if subErr != nil {
				errs.Append(errors.Warning("websocket: close sub protocol handler failed").WithCause(subErr).WithMeta("sub", name))
			}
		}
	}
	if len(errs) > 0 {
		err = errors.Warning("websocket: close failed").WithCause(errs.Error())
	}
	return
}

var (
	connectionId = []byte("@fns:websocket:connId")
)

func ConnectionId(ctx context.Context) (id string) {
	v := ctx.LocalValue(connectionId)
	if v == nil {
		return
	}
	id = v.(string)
	return
}
