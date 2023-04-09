package websockets

import (
	"context"
	"fmt"
	"github.com/aacfactory/configures"
	"github.com/aacfactory/errors"
	"github.com/aacfactory/fns-contrib/transports/handlers/websockets/websocket"
	"github.com/aacfactory/fns/commons/bytex"
	"github.com/aacfactory/fns/commons/uid"
	"github.com/aacfactory/fns/service"
	"github.com/aacfactory/fns/service/transports"
	"github.com/aacfactory/json"
	"github.com/aacfactory/logs"
	"github.com/aacfactory/workers"
	"io"
	"time"
	"unicode/utf8"
)

const (
	handleName = "websockets"
)

func Websocket(subs ...SubProtocolHandler) (handler service.TransportHandler) {
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
	appId                 string
	log                   logs.Logger
	upgrader              *websocket.Upgrader
	service               *Service
	discovery             service.EndpointDiscovery
	subs                  map[string]SubProtocolHandler
	workers               workers.Workers
	readTimeout           time.Duration
	writeTimeout          time.Duration
	maxRequestMessageSize int64
	originCheckFunc       func(r *transports.Request) bool
}

func (handler *websocketHandler) Name() (name string) {
	name = handleName
	return
}

func (handler *websocketHandler) Build(options service.TransportHandlerOptions) (err error) {
	handler.appId = options.AppId
	handler.log = options.Log

	config := Config{}
	configErr := options.Config.As(&config)
	if configErr != nil {
		err = errors.Warning("websocket: build failed").WithCause(configErr)
		return
	}
	handshakeTimeout := time.Duration(0)
	if config.HandshakeTimeout != "" {
		handshakeTimeout, err = time.ParseDuration(config.HandshakeTimeout)
		if err != nil {
			err = errors.Warning("websocket: build failed").WithCause(errors.Warning("handshakeTimeout format must be time.Duration"))
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
		Error: func(w transports.ResponseWriter, r *transports.Request, status int, reason error) {
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
				AppId:                 options.AppId,
				AppName:               options.AppName,
				AppVersion:            options.AppVersion,
				Log:                   options.Log.With("protocol", name),
				Config:                subConfig,
				ReadTimeout:           readTimeout,
				WriteTimeout:          writeTimeout,
				MaxRequestMessageSize: handler.maxRequestMessageSize,
				Discovery:             options.Discovery,
			}
			subErr := sub.Build(subOptions)
			if subErr != nil {
				err = errors.Warning("websocket: build failed").WithCause(subErr).WithMeta("sub", name)
				return
			}
		}
	}

	handler.discovery = options.Discovery

	maxConnections := config.MaxConnections
	if maxConnections == 0 {
		maxConnections = 10240
	}
	handler.workers = workers.New(workers.MaxWorkers(maxConnections))

	return
}

func (handler *websocketHandler) Accept(request *transports.Request) (ok bool) {
	if !request.IsGet() {
		return
	}
	ok = websocket.IsWebSocketUpgrade(request)
	return
}

func (handler *websocketHandler) Handle(writer transports.ResponseWriter, request *transports.Request) {
	upgradeErr := handler.upgrader.Upgrade(writer, request, handler.handleConn)
	if upgradeErr != nil {
		writer.Failed(errors.Warning("websocket: upgrade failed").WithCause(upgradeErr))
		return
	}
	return
}

func (handler *websocketHandler) handleConn(ctx context.Context, conn *websocket.Conn, header transports.Header) {
	conn.SetPingHandler(nil)
	conn.SetPongHandler(func(appData string) error {
		return conn.WriteControl(websocket.PongMessage, bytex.FromString("pong"), time.Now().Add(2*time.Second))
	})
	protocol := header.Get("Sec-Websocket-Protocol")
	if protocol != "" {
		sub, has := handler.subs[protocol]
		if !has {
			_ = conn.WriteControl(websocket.CloseMessage, websocket.FormatCloseMessage(websocket.CloseProtocolError, "sub protocol is unsupported"), time.Now().Add(2*time.Second))
			_ = conn.Close()
			return
		}
		sub.Handle(ctx, &WebsocketConnection{
			Conn:     conn,
			header:   header,
			deviceId: header.Get("X-Fns-Device-Id"),
			deviceIp: header.Get("X-Fns-Device-Ip"),
		})
		return
	}
	handler.handle(ctx, conn, header)
	return
}

func (handler *websocketHandler) handle(ctx context.Context, conn *websocket.Conn, header transports.Header) {
	deviceId := header.Get("X-Fns-Device-Id")
	deviceIp := header.Get("X-Fns-Device-Ip")
	id, mountErr := handler.mount(ctx, conn, deviceId)
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
	}(ctx, handler, id, deviceId)
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
		ctx = context.WithValue(ctx, connectionId, id)
		endpoint, has := handler.discovery.Get(ctx, request.Service)
		if !has {
			writeErr := conn.WriteMessage(websocket.TextMessage, Failed(errors.NotFound("websockets: service was not found").WithMeta("service", request.Service)).Encode())
			if writeErr != nil {
				break
			}
			continue
		}

		result, requestErr := endpoint.RequestSync(ctx, service.NewRequest(
			ctx,
			request.Service, request.Fn,
			service.NewArgument(request.Payload),
			service.WithRequestHeader(request.Header),
			service.WithDeviceId(deviceId),
			service.WithDeviceIp(deviceIp),
			service.WithRequestId(uid.UID()),
			service.WithRequestVersions(rvs),
		))
		if requestErr != nil {
			writeErr := conn.WriteMessage(websocket.TextMessage, Failed(requestErr).Encode())
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
	_ = handler.unmount(ctx, id, deviceId)
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

func (handler *websocketHandler) Services() (services []service.Service) {
	services = make([]service.Service, 0, 1)
	services = append(services, handler.service)
	if handler.subs != nil && len(handler.subs) > 0 {
		for _, sub := range handler.subs {
			subService := sub.Service()
			if subService != nil {
				services = append(services, subService)
			}
		}
	}
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

const (
	connectionId = "fns@websocketId"
)

func ConnectionId(ctx context.Context) (id string) {
	v := ctx.Value(connectionId)
	if v == nil {
		return
	}
	id = v.(string)
	return
}
