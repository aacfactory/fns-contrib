package websockets

import (
	"github.com/aacfactory/fns/service"
	"net/http"
)

const (
	handleName = "websockets"
)

type websocketHandler struct {
}

func (handler *websocketHandler) ServeHTTP(writer http.ResponseWriter, request *http.Request) {
	//TODO implement me
	panic("implement me")
}

func (handler *websocketHandler) Name() (name string) {
	name = handleName
	return
}

func (handler *websocketHandler) Build(options *service.HttpHandlerOptions) (err error) {
	//TODO implement me
	panic("implement me")
}

func (handler *websocketHandler) Accept(request *http.Request) (ok bool) {
	//TODO implement me
	panic("implement me")
}

func (handler *websocketHandler) Close() {
	//TODO implement me
	panic("implement me")
}
