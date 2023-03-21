package mqtt

import (
	"context"
	"github.com/aacfactory/errors"
	"github.com/aacfactory/fns/commons/uid"
	"github.com/aacfactory/fns/service"
	"sync"
)

const (
	mountFn   = "mount"
	unmountFn = "unmount"
)

func newService() *Service {
	return &Service{
		Abstract: service.NewAbstract(_name, true),
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

	return
}

func (svc *Service) mount(conn *Connection) (id string) {
	id = uid.UID()
	svc.conns.Store(id, conn)
	return
}

func (svc *Service) unmount(id string) {
	svc.conns.Delete(id)
}

func (svc *Service) getConn(id string) (conn *Connection, has bool) {
	v, exist := svc.conns.Load(id)
	if !exist {
		return
	}
	conn, has = v.(*Connection)
	return
}

type mountParam struct {
	ConnectionId string `json:"connectionId"`
	AppId        string `json:"appId"`
}

type unmountParam struct {
	ConnectionId string `json:"connectionId"`
}
