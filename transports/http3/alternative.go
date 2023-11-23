package http3

import (
	"github.com/aacfactory/errors"
	"github.com/aacfactory/fns/commons/bytex"
	"github.com/aacfactory/fns/transports"
	"github.com/quic-go/quic-go/http3"
	"net/http"
)

var (
	altSvcHeaderName = []byte("Alt-Svc")
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

func (handler *alternativeHandler) Handle(writer transports.ResponseWriter, request transports.Request) {
	header := http.Header{}
	headerErr := handler.quic.SetQuicHeaders(header)
	if headerErr != nil {
		writer.Failed(errors.Warning("http3: announce that this server supports HTTP/3 failed").WithCause(headerErr))
		return
	}
	hvv := header.Values(bytex.ToString(altSvcHeaderName))
	if len(hvv) > 0 {
		for _, h := range hvv {
			writer.Header().Add(altSvcHeaderName, bytex.FromString(h))
		}
	}
	handler.handler.Handle(writer, request)
	return
}
