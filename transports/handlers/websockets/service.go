package websockets

import (
	"fmt"
	"github.com/aacfactory/errors"
	"github.com/aacfactory/fns-contrib/transports/handlers/websockets/websocket"
	"github.com/aacfactory/fns/context"
	"github.com/aacfactory/fns/services"
	"time"
)

var (
	_endpointName = handleName
)

func newService(registration Registration) *service {
	return &service{
		Abstract:     services.NewAbstract(string(_endpointName), true, registration),
		registration: registration,
	}
}

type service struct {
	services.Abstract
	conns        *Connections
	registration Registration
}

func (svc *service) Construct(options services.Options) (err error) {
	if err = svc.Abstract.Construct(options); err != nil {
		return
	}

	svc.conns = &Connections{}
	svc.AddFunction(&sendFn{
		conns:        svc.conns,
		registration: svc.registration,
	})
	return
}

func (svc *service) mount(ctx context.Context, conn *websocket.Conn, endpointId []byte, ttl time.Duration) (err error) {
	if len(endpointId) == 0 {
		err = errors.Warning("websockets: mount connection failed").WithCause(fmt.Errorf("host app id is not found"))
		return
	}

	connId := conn.Id()
	svc.conns.Set(conn)
	setErr := svc.registration.Set(ctx, connId, endpointId, ttl)
	if setErr != nil {
		svc.conns.Remove(connId)
		err = errors.Warning("websockets: mount connection failed").WithCause(setErr)
		return
	}
	return
}

func (svc *service) unmount(ctx context.Context, conn *websocket.Conn) (err error) {
	connId := conn.Id()
	svc.conns.Remove(connId)
	removeErr := svc.registration.Remove(ctx, connId)
	if removeErr != nil {
		err = errors.Warning("websockets: unmount connection failed").WithCause(removeErr)
		return
	}
	return
}

func (svc *service) refreshTTL(ctx context.Context, id []byte, endpointId []byte, ttl time.Duration) {
	_ = svc.registration.Set(ctx, id, endpointId, ttl)
}
