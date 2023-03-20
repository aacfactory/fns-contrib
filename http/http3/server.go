package http3

import (
	"fmt"
	"github.com/aacfactory/errors"
	"github.com/aacfactory/fns/commons/bytex"
	"github.com/aacfactory/fns/service"
	"github.com/aacfactory/json"
	"github.com/aacfactory/logs"
	"github.com/quic-go/quic-go/http3"
	"net/http"
	"strings"
	"time"
)

const (
	httpContentType     = "Content-Type"
	httpContentTypeJson = "application/json"
)

func Server() service.Http {
	return &server{}
}

type server struct {
	log    logs.Logger
	std    *http.Server
	quic   *http3.Server
	dialer *Dialer
}

func (srv *server) Name() (name string) {
	name = "http3"
	return
}

func (srv *server) Build(options service.HttpOptions) (err error) {
	srvTLS := options.ServerTLS
	if srvTLS == nil {
		err = errors.Warning("http3: build failed").WithCause(errors.Warning("tls is required"))
		return
	}
	srv.log = options.Log
	config := Config{}
	if options.Options != nil && len(options.Options) > 2 {
		decodeErr := json.Unmarshal(options.Options, &config)
		if decodeErr != nil {
			err = errors.Warning("http3: build failed").WithCause(decodeErr)
			return
		}
	}
	maxHeaderBytes := uint64(0)
	if config.MaxHeaderBytes != "" {
		maxHeaderBytes, err = bytex.ToBytes(strings.TrimSpace(config.MaxHeaderBytes))
		if err != nil {
			err = errors.Warning("http3: build failed").WithCause(errors.Warning("maxHeaderBytes is invalid").WithCause(err).WithMeta("hit", "format must be bytes"))
			return
		}
	}
	quicConfig, quicConfigErr := config.QuicConfig()
	if quicConfigErr != nil {
		err = errors.Warning("http3: build failed").WithCause(quicConfigErr)
		return
	}
	// server
	handler := options.Handler
	srv.quic = &http3.Server{
		Addr:               fmt.Sprintf(":%d", options.Port),
		Port:               options.Port,
		TLSConfig:          http3.ConfigureTLSConfig(srvTLS),
		QuicConfig:         quicConfig,
		Handler:            handler,
		EnableDatagrams:    config.EnableDatagrams,
		MaxHeaderBytes:     int(maxHeaderBytes),
		AdditionalSettings: config.AdditionalSettings,
		StreamHijacker:     nil,
		UniStreamHijacker:  nil,
	}
	// std
	if config.EnableTCP {
		srv.std = &http.Server{
			Addr: fmt.Sprintf(":%d", options.Port),
			Handler: http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
				headerErr := srv.quic.SetQuicHeaders(writer.Header())
				if headerErr != nil {
					headerErr = errors.Warning("http3: announce that this server supports HTTP/3 failed").WithCause(headerErr)
					p, _ := json.Marshal(headerErr)
					writer.WriteHeader(555)
					writer.Header().Set(httpContentType, httpContentTypeJson)
					_, _ = writer.Write(p)
					return
				}
				handler.ServeHTTP(writer, request)
				return
			}),
			DisableGeneralOptionsHandler: false,
			TLSConfig:                    srvTLS,
			ReadTimeout:                  10 * time.Second,
			ReadHeaderTimeout:            1 * time.Second,
			WriteTimeout:                 10 * time.Second,
			IdleTimeout:                  10 * time.Second,
			MaxHeaderBytes:               int(maxHeaderBytes),
			TLSNextProto:                 nil,
			ConnState:                    nil,
			ErrorLog:                     logs.MapToLogger(options.Log, logs.ErrorLevel, true),
			BaseContext:                  nil,
			ConnContext:                  nil,
		}

	}
	// dialer
	dialer, dialerErr := NewDialer(options.ClientTLS, config.ClientConfig(), config.EnableDatagrams, quicConfig, config.AdditionalSettings)
	if dialerErr != nil {
		err = errors.Warning("http3: build failed").WithCause(dialerErr)
		return
	}
	srv.dialer = dialer
	return
}

func (srv *server) Dial(address string) (client service.HttpClient, err error) {
	client, err = srv.dialer.Dial(address)
	return
}

func (srv *server) ListenAndServe() (err error) {
	if srv.std == nil {
		err = srv.quic.ListenAndServe()
		if err != nil {
			err = errors.Warning("http3: listen and serve failed").WithCause(err)
			return
		}
		return
	}
	sErr := make(chan error)
	qErr := make(chan error)
	go func(std *http.Server, sErr chan error) {
		sErr <- std.ListenAndServeTLS("", "")
	}(srv.std, sErr)
	go func(quic *http3.Server, qErr chan error) {
		qErr <- quic.ListenAndServe()
	}(srv.quic, qErr)
	select {
	case srvErr := <-sErr:
		_ = srv.Close()
		err = errors.Warning("http3: listen and serve failed").WithCause(srvErr).WithMeta("kind", "http")
		return
	case srvErr := <-qErr:
		_ = srv.Close()
		err = errors.Warning("http3: listen and serve failed").WithCause(srvErr).WithMeta("kind", "http3")
		return
	}
}

func (srv *server) Close() (err error) {
	srv.dialer.Close()
	if srv.std != nil {
		_ = srv.std.Close()
	}
	err = srv.quic.Close()
	if err != nil {
		err = errors.Warning("http3: close failed").WithCause(err)
	}
	return
}
