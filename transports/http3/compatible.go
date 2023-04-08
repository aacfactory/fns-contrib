package http3

import (
	"github.com/aacfactory/errors"
	"github.com/aacfactory/fns/service/transports"
	"github.com/quic-go/quic-go/http3"
	"net/http"
)

func newCompatibleHandler(quic *http3.Server, handler transports.Handler) *compatibleHandler {
	return &compatibleHandler{
		handler: handler,
		quic:    quic,
	}
}

type compatibleHandler struct {
	handler transports.Handler
	quic    *http3.Server
}

func (compatible *compatibleHandler) Handle(writer transports.ResponseWriter, request *transports.Request) {
	headerErr := compatible.quic.SetQuicHeaders(http.Header(writer.Header()))
	if headerErr != nil {
		writer.Failed(errors.Warning("http3: announce that this server supports HTTP/3 failed").WithCause(headerErr))
		return
	}
	compatible.handler.Handle(writer, request)
	return
}
