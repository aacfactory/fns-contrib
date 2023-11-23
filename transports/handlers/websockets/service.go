package websockets

import (
	"fmt"
	"github.com/aacfactory/errors"
	"github.com/aacfactory/fns-contrib/transports/handlers/websockets/websocket"
	"github.com/aacfactory/fns/context"
	"github.com/aacfactory/fns/services"
	"github.com/aacfactory/fns/services/commons"
	"time"
)

var (
	_endpointName = handleName
)

func newService(registration Registration) *service {
	return &service{
		Abstract: services.NewAbstract(string(_endpointName), true, registration, &Connections{}),
	}
}

type service struct {
	services.Abstract
}

func (svc *service) Construct(options services.Options) (err error) {
	if err = svc.Abstract.Construct(options); err != nil {
		return
	}
	svc.AddFunction(commons.NewFn(string(_sendFnName), false, false, true, false, false, false, sendFn))
	return
}

func (svc *service) mount(ctx context.Context, conn *websocket.Conn, endpointId []byte, ttl time.Duration) (err error) {
	if len(endpointId) == 0 {
		err = errors.Warning("websockets: mount connection failed").WithCause(fmt.Errorf("host app id is not found"))
		return
	}
	conns, hasConns := LoadConnections(ctx)
	if !hasConns {
		err = errors.Warning("websockets: mount connection failed").WithCause(fmt.Errorf("there is no connections in context"))
		return
	}
	registration, hasRegistration := LoadRegistration(ctx)
	if !hasRegistration {
		err = errors.Warning("websockets: mount connection failed").WithCause(fmt.Errorf("there is no registration in context"))
		return
	}
	connId := conn.Id()
	conns.Set(conn)
	setErr := registration.Set(ctx, connId, endpointId, ttl)
	if setErr != nil {
		conns.Remove(connId)
		err = errors.Warning("websockets: mount connection failed").WithCause(setErr)
		return
	}
	return
}

func (svc *service) unmount(ctx context.Context, conn *websocket.Conn) (err error) {
	conns, hasConns := LoadConnections(ctx)
	if !hasConns {
		err = errors.Warning("websockets: unmount connection failed").WithCause(fmt.Errorf("there is no connections in context"))
		return
	}
	registration, hasRegistration := LoadRegistration(ctx)
	if !hasRegistration {
		err = errors.Warning("websockets: unmount connection failed").WithCause(fmt.Errorf("there is no registration in context"))
		return
	}
	connId := conn.Id()
	conns.Remove(connId)
	removeErr := registration.Remove(ctx, connId)
	if removeErr != nil {
		err = errors.Warning("websockets: unmount connection failed").WithCause(removeErr)
		return
	}
	return
}

func (svc *service) refreshTTL(ctx context.Context, id []byte, endpointId []byte, ttl time.Duration) {
	registration, hasRegistration := LoadRegistration(ctx)
	if !hasRegistration {
		return
	}
	_ = registration.Set(ctx, id, endpointId, ttl)
}
