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

var (
	handleName = []byte("websockets")
)

var (
	ErrTooMayConnections = errors.New(http.StatusTooManyRequests, "***TOO MANY CONNECTIONS***", "fns: too may connections, try again later.")
)

type Options struct {
	subs         []SubProtocolHandler
	registration Registration
}

type Option func(options *Options)

func WithSubProtocolHandler(handler ...SubProtocolHandler) Option {
	return func(options *Options) {
		options.subs = append(options.subs, handler...)
	}
}

func WithRegistration(registration Registration) Option {
	return func(options *Options) {
		options.registration = registration
	}
}

func New(options ...Option) (handler transports.MuxHandler) {
	opt := Options{
		registration: &defaultRegistration{},
		subs:         nil,
	}
	for _, option := range options {
		option(&opt)
	}
	wh := &websocketHandler{
		log:     nil,
		subs:    make(map[string]SubProtocolHandler),
		service: newService(opt.registration),
	}
	if opt.subs != nil && len(opt.subs) > 0 {
		for _, sub := range opt.subs {
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
	subs                  map[string]SubProtocolHandler
	readTimeout           time.Duration
	writeTimeout          time.Duration
	maxRequestMessageSize int64
	maxConnections        int64
	connections           *atomic.Int64
	originCheckFunc       func(r *transports.Request) bool
	service               *service
	connectionTTL         time.Duration
	enableEcho            bool
}

func (handler *websocketHandler) Services() []services.Service {
	return []services.Service{handler.service}
}

func (handler *websocketHandler) Name() (name string) {
	name = string(handleName)
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
			err = errors.Warning("websocket: construct failed").WithCause(errors.Warning("readTimeout format must be time.Duration"))
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
			err = errors.Warning("websocket: construct failed").WithCause(errors.Warning("readBufferSize format must be byte"))
			return
		}
	}
	writeTimeout := time.Duration(0)
	if config.WriteTimeout != "" {
		writeTimeout, err = time.ParseDuration(config.WriteTimeout)
		if err != nil {
			err = errors.Warning("websocket: construct failed").WithCause(errors.Warning("writeTimeout format must be time.Duration"))
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
			err = errors.Warning("websocket: construct failed").WithCause(errors.Warning("writeBufferSize format must be byte"))
			return
		}
	}
	maxRequestMessageSize := uint64(0)
	if config.MaxRequestMessageSize != "" {
		maxRequestMessageSize, err = bytex.ParseBytes(config.MaxRequestMessageSize)
		if err != nil {
			err = errors.Warning("websocket: construct failed").WithCause(errors.Warning("maxRequestMessageSize format must be byte"))
			return
		}
	} else {
		maxRequestMessageSize = uint64(4096)
	}
	handler.maxRequestMessageSize = int64(maxRequestMessageSize)

	originCheckFn, originCheckFnErr := config.OriginCheckPolicy.Build()
	if originCheckFnErr != nil {
		err = errors.Warning("websocket: construct failed").WithCause(originCheckFnErr)
		return
	}

	connectionTTL := time.Duration(0)
	if config.ConnectionTTL != "" {
		connectionTTL, err = time.ParseDuration(config.ConnectionTTL)
		if err != nil {
			err = errors.Warning("websocket: construct failed").WithCause(errors.Warning("connectionTTL format must be time.Duration"))
			return
		}
	}
	if connectionTTL < 1 {
		connectionTTL = 10 * time.Minute
	}
	handler.connectionTTL = connectionTTL
	handler.enableEcho = config.EnableEcho
	// upgrader
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
			subErr := sub.Construct(subOptions)
			if subErr != nil {
				err = errors.Warning("websocket: construct sub protocol handler failed").WithCause(subErr).WithMeta("sub", name)
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
	if handler.connections.Load() >= handler.maxConnections {
		w.Failed(ErrTooMayConnections)
		return
	}
	handler.connections.Add(1)
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
	conn.SetPingHandler(func(appData []byte) error {
		return conn.WriteControl(websocket.PongMessage, bytex.FromString("PONG"), time.Now().Add(2*time.Second))
	})
	conn.SetPongHandler(func(appData []byte) error {
		return conn.WriteControl(websocket.PongMessage, bytex.FromString("PONG"), time.Now().Add(2*time.Second))
	})
	deviceId := header.Get(transports.DeviceIdHeaderName)
	if len(deviceId) == 0 {
		deviceId = conn.Id()
	}
	deviceIp := transports.DeviceIp(ctx)
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
			deviceId: string(deviceId),
			deviceIp: string(deviceIp),
		})
		return
	}
	handler.handle(ctx, conn, deviceId, deviceIp)
	return
}

func (handler *websocketHandler) handle(ctx context.Context, conn *websocket.Conn, deviceId []byte, deviceIp []byte) {
	endpointId := runtime.AppId(ctx)
	// mount
	mountErr := handler.service.mount(ctx, conn, endpointId, handler.connectionTTL)
	if mountErr != nil {
		failed := Failed(mountErr)
		_ = conn.WriteControl(websocket.CloseMessage, failed.Encode(), time.Now().Add(2*time.Second))
		_ = conn.Close()
		return
	}

	connId := conn.Id()
	// with conn id
	withConnectionId(ctx, connId)

	// handle
	for {
		handler.service.refreshTTL(ctx, connId, endpointId, handler.connectionTTL)
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
		if mt == websocket.PingMessage {
			wErr := conn.WriteControl(websocket.PongMessage, bytex.FromString("PONG"), time.Now().Add(2*time.Second))
			if wErr != nil {
				break
			}
			continue
		}
		if mt == websocket.BinaryMessage {
			_ = conn.WriteControl(websocket.CloseMessage, websocket.FormatCloseMessage(websocket.CloseInvalidFramePayloadData, "binary message was unsupported"), time.Now().Add(2*time.Second))
			break
		}
		message, readErr := readMessage(reader, handler.maxRequestMessageSize)
		if readErr != nil {
			if errors.Contains(readErr, ErrRequestMessageIsTooLarge) {
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
		// echo
		if handler.enableEcho && bytes.Equal(message, []byte("echo")) {
			wErr := conn.WriteMessage(websocket.TextMessage, Succeed(time.Now().Format(time.RFC3339)).Encode())
			if wErr != nil {
				break
			}
			continue
		}
		if bytes.Equal(message, []byte("PING")) {
			wErr := conn.WriteMessage(websocket.TextMessage, []byte("PONG"))
			if wErr != nil {
				break
			}
			continue
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
		// request
		requestOptions := make([]services.RequestOption, 0, 1)
		requestOptions = append(requestOptions, services.WithRequestId(uid.Bytes()))
		requestOptions = append(requestOptions, services.WithDeviceIp(deviceId))
		requestOptions = append(requestOptions, services.WithDeviceIp(deviceIp))
		token := request.Authorization()
		if len(token) > 0 {
			requestOptions = append(requestOptions, services.WithToken(token))
		}
		rvs, hasVersion, parseVersionErr := request.Versions()
		if parseVersionErr != nil {
			_ = conn.WriteControl(websocket.CloseMessage, websocket.FormatCloseMessage(websocket.CloseInvalidFramePayloadData, "parse X-Fns-Request-Version failed"), time.Now().Add(2*time.Second))
			break
		}
		if hasVersion {
			requestOptions = append(requestOptions, services.WithRequestVersions(rvs))
		}
		eps := runtime.Endpoints(ctx)
		reqCtx := context.Acquire(ctx)
		transports.WithRequestHeader(reqCtx, request.Header)
		WithConnection(ctx, conn)
		response, handleErr := eps.Request(
			reqCtx, bytex.FromString(request.Endpoint), bytex.FromString(request.Fn), request.Payload,
			requestOptions...,
		)
		context.Release(reqCtx)
		if handleErr != nil {
			writeErr := conn.WriteMessage(websocket.TextMessage, Failed(handleErr).Encode())
			if writeErr != nil {
				break
			}
			continue
		}
		if !response.Valid() {
			writeErr := conn.WriteMessage(websocket.TextMessage, Succeed(nil).Encode())
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
	// unmount
	_ = handler.service.unmount(ctx, conn)
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
	connectionIdContextKey = []byte("@fns:websocket:connectionId")
)

func withConnectionId(ctx context.Context, id []byte) {
	ctx.SetLocalValue(connectionIdContextKey, id)
}

func ConnectionId(ctx context.Context) (id []byte) {
	v := ctx.LocalValue(connectionIdContextKey)
	if v == nil {
		return
	}
	id = v.([]byte)
	return
}
