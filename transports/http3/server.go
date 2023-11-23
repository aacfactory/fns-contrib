package http3

import (
	"crypto/tls"
	"fmt"
	"github.com/aacfactory/errors"
	"github.com/aacfactory/fns/commons/bytex"
	"github.com/aacfactory/fns/context"
	"github.com/aacfactory/fns/transports"
	"github.com/aacfactory/fns/transports/standard"
	"github.com/quic-go/quic-go"
	"github.com/quic-go/quic-go/http3"
	"strings"
	"time"
)

func newServer(port int, srvTLS *tls.Config, config *Config, quicConfig *quic.Config, handler transports.Handler) (srv *Server, err error) {
	if srvTLS == nil {
		err = errors.Warning("http3: build server failed").WithCause(errors.Warning("tls is required"))
		return
	}
	srvTLS.NextProtos = []string{"h3", "h3-29"}

	maxRequestHeaderSize := uint64(0)
	if config.MaxRequestHeaderSize != "" {
		maxRequestHeaderSize, err = bytex.ParseBytes(strings.TrimSpace(config.MaxRequestHeaderSize))
		if err != nil {
			err = errors.Warning("http3: build server failed").WithCause(errors.Warning("maxRequestHeaderSize is invalid").WithCause(err).WithMeta("hit", "format must be bytes"))
			return
		}
	}
	maxRequestBodySize := uint64(0)
	if config.MaxRequestBodySize != "" {
		maxRequestBodySize, err = bytex.ParseBytes(strings.TrimSpace(config.MaxRequestBodySize))
		if err != nil {
			err = errors.Warning("http3: build server failed").WithCause(errors.Warning("maxRequestBodySize is invalid").WithCause(err).WithMeta("hit", "format must be bytes"))
			return
		}
	}
	if maxRequestBodySize == 0 {
		maxRequestBodySize = 4 * bytex.MEGABYTE
	}

	// server
	server := &http3.Server{
		Addr:               fmt.Sprintf(":%d", port),
		Port:               port,
		TLSConfig:          srvTLS,
		QuicConfig:         quicConfig,
		Handler:            standard.HttpTransportHandlerAdaptor(handler, int(maxRequestBodySize), 30*time.Second),
		EnableDatagrams:    config.EnableDatagrams,
		MaxHeaderBytes:     int(maxRequestHeaderSize),
		AdditionalSettings: config.AdditionalSettings,
		StreamHijacker:     nil,
		UniStreamHijacker:  nil,
	}

	srv = &Server{
		port: port,
		srv:  server,
	}

	return
}

type Server struct {
	port int
	srv  *http3.Server
}

func (srv *Server) Shutdown(_ context.Context) (err error) {
	err = srv.srv.Close()
	if err != nil {
		err = errors.Warning("http3: server shutdown failed").WithCause(err).WithMeta("transport", transportName)
	}
	return
}
