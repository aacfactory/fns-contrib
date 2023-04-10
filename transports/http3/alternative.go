package http3

import (
	"github.com/aacfactory/errors"
	"github.com/aacfactory/fns/service/transports"
	"github.com/quic-go/quic-go/http3"
	"net/http"
)

func newAlternativeHandler(quic *http3.Server, handler transports.Handler) *alternativeHandler {
	return &alternativeHandler{
		handler: handler,
		quic:    quic,
	}
}

type alternativeHandler struct {
	handler transports.Handler
	quic    *http3.Server
}

func (handler *alternativeHandler) Handle(writer transports.ResponseWriter, request *transports.Request) {
	headerErr := handler.quic.SetQuicHeaders(http.Header(writer.Header()))
	if headerErr != nil {
		writer.Failed(errors.Warning("http3: announce that this server supports HTTP/3 failed").WithCause(headerErr))
		return
	}
	writer.Header().Set("x-quic", "h3")
	handler.handler.Handle(writer, request)
	return
}
