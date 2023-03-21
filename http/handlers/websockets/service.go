package websockets

import (
	"context"
	"github.com/aacfactory/errors"
	"github.com/aacfactory/fns/commons/uid"
	"github.com/aacfactory/fns/service"
	"github.com/fasthttp/websocket"
	"sync"
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

func (svc *Service) Handle(context context.Context, fn string, argument service.Argument) (v interface{}, err errors.CodeError) {

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
