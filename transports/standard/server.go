package standard

import (
	"fmt"
	"github.com/aacfactory/errors"
	"github.com/aacfactory/fns/commons/bytex"
	"github.com/aacfactory/fns/service/transports"
	"github.com/aacfactory/logs"
	"net/http"
	"strings"
	"time"
)

func Server() transports.Transport {
	return &server{}
}

type server struct {
	log    logs.Logger
	srv    *http.Server
	dialer *Dialer
}

func (srv *server) Name() (name string) {
	name = "http"
	return
}

func (srv *server) Build(options transports.Options) (err error) {
	srvTLS := options.ServerTLS
	srv.log = options.Log
	config := Config{}
	decodeErr := options.Config.As(&config)
	if decodeErr != nil {
		err = errors.Warning("http: build failed").WithCause(decodeErr)
		return
	}
	maxRequestHeaderSize := uint64(0)
	if config.MaxRequestHeaderSize != "" {
		maxRequestHeaderSize, err = bytex.ParseBytes(strings.TrimSpace(config.MaxRequestHeaderSize))
		if err != nil {
			err = errors.Warning("http: build failed").WithCause(errors.Warning("maxRequestHeaderSize is invalid").WithCause(err).WithMeta("hit", "format must be bytes"))
			return
		}
	}
	maxRequestBodySize := uint64(0)
	if config.MaxRequestBodySize != "" {
		maxRequestBodySize, err = bytex.ParseBytes(strings.TrimSpace(config.MaxRequestBodySize))
		if err != nil {
			err = errors.Warning("http: build failed").WithCause(errors.Warning("maxRequestBodySize is invalid").WithCause(err).WithMeta("hit", "format must be bytes"))
			return
		}
	}
	if maxRequestBodySize == 0 {
		maxRequestBodySize = 4 * bytex.MEGABYTE
	}
	readTimeout := 10 * time.Second
	if config.ReadTimeout != "" {
		readTimeout, err = time.ParseDuration(strings.TrimSpace(config.ReadTimeout))
		if err != nil {
			err = errors.Warning("http: build failed").WithCause(errors.Warning("readTimeout is invalid").WithCause(err).WithMeta("hit", "format must time.Duration"))
			return
		}
	}
	readHeaderTimeout := 5 * time.Second
	if config.ReadHeaderTimeout != "" {
		readHeaderTimeout, err = time.ParseDuration(strings.TrimSpace(config.ReadHeaderTimeout))
		if err != nil {
			err = errors.Warning("http: build failed").WithCause(errors.Warning("readHeaderTimeout is invalid").WithCause(err).WithMeta("hit", "format must time.Duration"))
			return
		}
	}
	writeTimeout := 30 * time.Second
	if config.WriteTimeout != "" {
		writeTimeout, err = time.ParseDuration(strings.TrimSpace(config.WriteTimeout))
		if err != nil {
			err = errors.Warning("http: build failed").WithCause(errors.Warning("writeTimeout is invalid").WithCause(err).WithMeta("hit", "format must time.Duration"))
			return
		}
	}
	idleTimeout := 30 * time.Second
	if config.IdleTimeout != "" {
		idleTimeout, err = time.ParseDuration(strings.TrimSpace(config.IdleTimeout))
		if err != nil {
			err = errors.Warning("http: build failed").WithCause(errors.Warning("idleTimeout is invalid").WithCause(err).WithMeta("hit", "format must time.Duration"))
			return
		}
	}
	// server
	handler := transports.HttpTransportHandlerAdaptor(options.Handler, int(maxRequestBodySize))
	srv.srv = &http.Server{
		Addr:                         fmt.Sprintf(":%d", options.Port),
		Handler:                      handler,
		DisableGeneralOptionsHandler: false,
		TLSConfig:                    srvTLS,
		ReadTimeout:                  readTimeout,
		ReadHeaderTimeout:            readHeaderTimeout,
		WriteTimeout:                 writeTimeout,
		IdleTimeout:                  idleTimeout,
		MaxHeaderBytes:               int(maxRequestHeaderSize),
		ErrorLog:                     logs.MapToLogger(srv.log, logs.DebugLevel, false),
	}
	// dialer
	dialer, dialerErr := NewDialer(options.ClientTLS, config.ClientConfig())
	if dialerErr != nil {
		err = errors.Warning("http: build failed").WithCause(dialerErr)
		return
	}
	srv.dialer = dialer
	return
}

func (srv *server) Dial(address string) (client transports.Client, err error) {
	client, err = srv.dialer.Dial(address)
	return
}

func (srv *server) ListenAndServe() (err error) {
	if srv.srv.TLSConfig == nil {
		err = srv.srv.ListenAndServe()
	} else {
		err = srv.srv.ListenAndServeTLS("", "")
	}

	if err != nil {
		err = errors.Warning("http: listen and serve failed").WithCause(err)
		return
	}
	return
}

func (srv *server) Close() (err error) {
	srv.dialer.Close()
	err = srv.srv.Close()
	if err != nil {
		err = errors.Warning("http: close failed").WithCause(err)
	}
	return
}
