package websockets

import (
	"context"
	"fmt"
	"github.com/aacfactory/configures"
	"github.com/aacfactory/errors"
	"github.com/aacfactory/fns-contrib/transports/handlers/websockets/websocket"
	"github.com/aacfactory/fns/commons/bytex"
	"github.com/aacfactory/fns/service"
	"github.com/aacfactory/fns/service/transports"
	"github.com/aacfactory/logs"
	"github.com/aacfactory/workers"
	"time"
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
	deviceId := header.Get("X-Fns-Device-Id")
	deviceIp := header.Get("X-Fns-Device-Ip")
	conn.SetPingHandler(nil)
	conn.SetPongHandler(func(appData string) error {
		return conn.WriteControl(websocket.PongMessage, bytex.FromString("pong"), time.Now().Add(2*time.Second))
	})
	protocol := header.Get("Sec-Websocket-Protocol")
	if protocol == "" {
		task := &Task{
			AbstractLongTask:      workers.NewAbstractLongTask(handler.readTimeout),
			appId:                 handler.appId,
			deviceId:              deviceId,
			deviceIp:              deviceIp,
			conn:                  conn,
			discovery:             handler.discovery,
			service:               handler.service,
			readTimeout:           handler.readTimeout,
			writeTimeout:          handler.writeTimeout,
			maxRequestMessageSize: handler.maxRequestMessageSize,
		}
		task.Execute(ctx)
	}
	return
}

func (handler *websocketHandler) handleConn1(ctx context.Context, conn *websocket.Conn, header transports.Header) {
	deviceId := header.Get("X-Fns-Device-Id")
	deviceIp := header.Get("X-Fns-Device-Ip")
	conn.SetPingHandler(nil)
	conn.SetPongHandler(func(appData string) error {
		return conn.WriteControl(websocket.PongMessage, bytex.FromString("pong"), time.Now().Add(2*time.Second))
	})
	protocol := header.Get("Sec-Websocket-Protocol")
	if protocol == "" {
		dispatched := handler.workers.Dispatch(ctx, &Task{
			AbstractLongTask:      workers.NewAbstractLongTask(handler.readTimeout),
			appId:                 handler.appId,
			deviceId:              deviceId,
			deviceIp:              deviceIp,
			conn:                  conn,
			discovery:             handler.discovery,
			service:               handler.service,
			readTimeout:           handler.readTimeout,
			writeTimeout:          handler.writeTimeout,
			maxRequestMessageSize: handler.maxRequestMessageSize,
		})
		if !dispatched {
			_ = conn.WriteControl(websocket.CloseMessage, websocket.FormatCloseMessage(websocket.CloseTryAgainLater, "no more connection worker"), time.Now().Add(2*time.Second))
			_ = conn.Close()
		}
		return
	}
	sub, has := handler.subs[protocol]
	if !has {
		_ = conn.Close()
		if handler.log.WarnEnabled() {
			handler.log.Warn().Message(fmt.Sprintf("%+v", errors.Warning("websocket: handler of Sec-Websocket-Protocol was not found").WithMeta("Sec-Websocket-Protocol", protocol)))
		}
		return
	}
	dispatched := handler.workers.Dispatch(ctx, &SubProtocolHandlerTask{
		handler: sub,
		conn: &WebsocketConnection{
			Conn:     conn,
			deviceId: deviceId,
			deviceIp: deviceIp,
		},
	})
	if !dispatched {
		_ = conn.WriteControl(websocket.CloseMessage, websocket.FormatCloseMessage(websocket.CloseTryAgainLater, "no more connection worker"), time.Now().Add(2*time.Second))
		_ = conn.Close()
	}
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
