package websockets

import (
	"github.com/aacfactory/fns-contrib/transports/handlers/websockets/websocket"
	"github.com/aacfactory/fns/commons/mmhash"
	"github.com/aacfactory/fns/context"
	"github.com/aacfactory/fns/services"
	"sync"
)

var (
	connectionContextKey = []byte("@fns:websockets:connection")
)

func WithConnection(ctx context.Context, conn *websocket.Conn) {
	ctx.SetLocalValue(connectionContextKey, conn)
}

func LoadConnection(ctx context.Context) (conn *websocket.Conn, has bool) {
	v := ctx.LocalValue(connectionContextKey)
	if v == nil {
		return
	}
	conn, has = v.(*websocket.Conn)
	return
}

func LoadConnections(ctx context.Context) (conns *Connections, has bool) {
	conns, has = services.LoadComponent[*Connections](ctx, _endpointName, connectionsComponentName)
	return
}

const (
	connectionsComponentName = "connections"
)

type Connections struct {
	values sync.Map
}

func (conns *Connections) Name() (name string) {
	name = connectionsComponentName
	return
}

func (conns *Connections) Construct(_ services.Options) (err error) {
	return
}

func (conns *Connections) Shutdown(_ context.Context) {
	return
}

func (conns *Connections) Get(id []byte) (conn *websocket.Conn, has bool) {
	v, exist := conns.values.Load(mmhash.Sum64(id))
	if !exist {
		return
	}
	conn, has = v.(*websocket.Conn)
	return
}

func (conns *Connections) Set(conn *websocket.Conn) {
	conns.values.Store(mmhash.Sum64(conn.Id()), conn)
	return
}

func (conns *Connections) Remove(id []byte) {
	conns.values.Delete(mmhash.Sum64(id))
	return
}
