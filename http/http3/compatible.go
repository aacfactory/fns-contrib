package http3

import (
	"github.com/aacfactory/errors"
	"github.com/aacfactory/json"
	"github.com/quic-go/quic-go/http3"
	"net/http"
)

func newCompatibleHandler(quic *http3.Server, handler http.Handler) *compatibleHandler {
	return &compatibleHandler{
		handler: handler,
		quic:    quic,
	}
}

type compatibleHandler struct {
	handler http.Handler
	quic    *http3.Server
}

func (compatible *compatibleHandler) ServeHTTP(writer http.ResponseWriter, request *http.Request) {
	headerErr := compatible.quic.SetQuicHeaders(writer.Header())
	if headerErr != nil {
		headerErr = errors.Warning("http3: announce that this server supports HTTP/3 failed").WithCause(headerErr)
		p, _ := json.Marshal(headerErr)
		writer.WriteHeader(555)
		writer.Header().Set(httpContentType, httpContentTypeJson)
		_, _ = writer.Write(p)
		return
	}
	compatible.handler.ServeHTTP(writer, request)
	return
}
