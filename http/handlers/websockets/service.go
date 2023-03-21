package websockets

import (
	"context"
	"fmt"
	"github.com/aacfactory/errors"
	"github.com/aacfactory/fns/commons/bytex"
	"github.com/aacfactory/fns/commons/uid"
	"github.com/aacfactory/fns/service"
	"github.com/fasthttp/websocket"
	"sync"
)

const (
	sendFn    = "send"
	mountFn   = "mount"
	unmountFn = "unmount"
)

func newService() *Service {
	return &Service{
		Abstract: service.NewAbstract(handleName, true),
		conns:    sync.Map{},
	}
}

type Service struct {
	service.Abstract
	conns sync.Map
}

func (svc *Service) Build(options service.Options) (err error) {
	err = svc.Abstract.Build(options)
	return
}

func (svc *Service) Components() (components map[string]service.Component) {
	return
}

func (svc *Service) Document() (doc service.Document) {
	return
}

func (svc *Service) Handle(ctx context.Context, fn string, argument service.Argument) (v interface{}, err errors.CodeError) {
	switch fn {
	case sendFn:
		param := sendParam{}
		paramErr := argument.As(&param)
		if paramErr != nil {
			err = errors.Warning("websockets: decode request argument failed").WithCause(paramErr)
			return
		}
		connId := param.ConnectionId
		if connId == "" {
			err = errors.BadRequest("websockets: connection id is required")
			return
		}
		payload := param.Payload
		if payload == nil || len(payload) == 0 {
			err = errors.BadRequest("websockets: payload is required")
			return
		}
		conn, has := svc.getConn(connId)
		if !has {
			err = errors.Warning("websockets: connection is lost").WithMeta("connection", connId)
			return
		}
		writeErr := conn.WriteMessage(websocket.TextMessage, payload)
		if writeErr != nil {
			err = errors.Warning("websockets: send failed").WithMeta("connection", connId).WithCause(writeErr)
			return
		}
		break
	case mountFn:
		param := mountParam{}
		paramErr := argument.As(&param)
		if paramErr != nil {
			err = errors.Warning("websockets: decode request argument failed").WithCause(paramErr)
			return
		}
		connId := param.ConnectionId
		if connId == "" {
			err = errors.BadRequest("websockets: connection id is required")
			return
		}
		store := service.SharedStore(ctx)
		setErr := store.Set(ctx, bytex.FromString(fmt.Sprintf("fns/websockets/%s", connId)), bytex.FromString(param.AppId))
		if setErr != nil {
			err = errors.ServiceError("websockets: mount failed").WithMeta("connection", connId).WithCause(setErr)
			return
		}
		break
	case unmountFn:
		param := unmountParam{}
		paramErr := argument.As(&param)
		if paramErr != nil {
			err = errors.Warning("websockets: decode request argument failed").WithCause(paramErr)
			return
		}
		connId := param.ConnectionId
		if connId == "" {
			err = errors.BadRequest("websockets: connection id is required")
			return
		}
		store := service.SharedStore(ctx)
		rmErr := store.Remove(ctx, bytex.FromString(fmt.Sprintf("fns/websockets/%s", connId)))
		if rmErr != nil {
			err = errors.ServiceError("websockets: unmount failed").WithMeta("connection", connId).WithCause(rmErr)
			return
		}
		break
	default:
		err = errors.Warning("websockets: fn was not found").WithMeta("service", handleName).WithMeta("fn", fn)
		break
	}
	return
}

func (svc *Service) mount(conn *websocket.Conn) (id string) {
	id = uid.UID()
	svc.conns.Store(id, conn)
	return
}

func (svc *Service) unmount(id string) {
	svc.conns.Delete(id)
}

func (svc *Service) getConn(id string) (conn *websocket.Conn, has bool) {
	v, exist := svc.conns.Load(id)
	if !exist {
		return
	}
	conn, has = v.(*websocket.Conn)
	return
}

type mountParam struct {
	ConnectionId string `json:"connectionId"`
	AppId        string `json:"appId"`
}

type unmountParam struct {
	ConnectionId string `json:"connectionId"`
}
