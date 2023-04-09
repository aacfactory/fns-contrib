package websockets

import (
	"context"
	"github.com/aacfactory/configures"
	"github.com/aacfactory/fns/commons/versions"
	"github.com/aacfactory/fns/service"
	"github.com/aacfactory/logs"
	"github.com/fasthttp/websocket"
	"io"
	"net"
	"time"
)

type MessageType int

type SubProtocolHandlerOptions struct {
	AppId                 string
	AppName               string
	AppVersion            versions.Version
	Log                   logs.Logger
	Config                configures.Config
	ReadTimeout           time.Duration
	WriteTimeout          time.Duration
	MaxRequestMessageSize int64
	Discovery             service.EndpointDiscovery `json:"-"`
}

type SubProtocolHandler interface {
	Name() (name string)
	Build(options SubProtocolHandlerOptions) (err error)
	Handle(ctx context.Context, conn Connection)
	Service() (service service.Service)
	Close() (err error)
}

type Connection interface {
	ReadMessage() (messageType MessageType, p []byte, err error)
	WriteMessage(messageType MessageType, data []byte) (err error)
	WriteControl(messageType MessageType, data []byte, deadline time.Time) error
	NextWriter(messageType MessageType) (io.WriteCloser, error)
	SetWriteDeadline(t time.Time) error
	NextReader() (messageType MessageType, r io.Reader, err error)
	SetReadDeadline(t time.Time) error
	SetReadLimit(limit int64)
	CloseHandler() func(code int, text string) error
	SetCloseHandler(h func(code int, text string) error)
	PingHandler() func(appData string) error
	SetPingHandler(h func(appData string) error)
	PongHandler() func(appData string) error
	SetPongHandler(h func(appData string) error)
	UnderlyingConn() net.Conn
	EnableWriteCompression(enable bool)
	SetCompressionLevel(level int) error
	Subprotocol() (protocol string)
	Close() (err error)
	LocalAddr() net.Addr
	RemoteAddr() net.Addr
}

type WebsocketConnection struct {
	*websocket.Conn
}

func (conn *WebsocketConnection) ReadMessage() (messageType MessageType, p []byte, err error) {
	mt := 0
	mt, p, err = conn.Conn.ReadMessage()
	messageType = MessageType(mt)
	return
}

func (conn *WebsocketConnection) WriteMessage(messageType MessageType, data []byte) (err error) {
	err = conn.Conn.WriteMessage(int(messageType), data)
	return
}

func (conn *WebsocketConnection) NextWriter(messageType MessageType) (w io.WriteCloser, err error) {
	w, err = conn.Conn.NextWriter(int(messageType))
	return
}

func (conn *WebsocketConnection) WriteControl(messageType MessageType, data []byte, deadline time.Time) error {
	return conn.Conn.WriteControl(int(messageType), data, deadline)
}

func (conn *WebsocketConnection) NextReader() (messageType MessageType, r io.Reader, err error) {
	mt := 0
	mt, r, err = conn.Conn.NextReader()
	messageType = MessageType(mt)
	return
}

type SubProtocolHandlerTask struct {
	handler SubProtocolHandler
	conn    *WebsocketConnection
}

func (t *SubProtocolHandlerTask) Execute(ctx context.Context) {
	t.handler.Handle(ctx, t.conn)
}
