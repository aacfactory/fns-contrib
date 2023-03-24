package http2

import (
	"fmt"
	"github.com/aacfactory/errors"
	"github.com/aacfactory/fns/commons/bytex"
	"github.com/aacfactory/fns/service"
	"github.com/aacfactory/logs"
	"github.com/dgrr/http2"
	"github.com/valyala/fasthttp"
	"github.com/valyala/fasthttp/fasthttpadaptor"
	"net"
	"strings"
	"time"
)

func Server() service.Http {
	return &server{}
}

type server struct {
	log    logs.Logger
	fast   *fasthttp.Server
	ln     net.Listener
	dialer *Dialer
}

func (srv *server) Name() (name string) {
	name = "fasthttp2"
	return
}

func (srv *server) Build(options service.HttpOptions) (err error) {
	srv.log = options.Log
	if options.ServerTLS == nil {
		err = errors.Warning("http2: build failed").WithCause(errors.Warning("server tls config is required"))
		return
	}
	// ln
	ln, lnErr := net.Listen("tcp4", fmt.Sprintf(":%d", options.Port))
	if lnErr != nil {
		err = errors.Warning("http2: build failed").WithCause(lnErr)
		return
	}
	srv.ln = ln
	// opt
	opt := service.FastHttpOptions{}
	optErr := options.Options.As(&opt)
	if optErr != nil {
		err = errors.Warning("http2: build failed").WithCause(optErr)
		return
	}

	readBufferSize := uint64(0)
	if opt.ReadBufferSize != "" {
		readBufferSize, err = bytex.ToBytes(strings.TrimSpace(opt.ReadBufferSize))
		if err != nil {
			err = errors.Warning("fns: build server failed").WithCause(errors.Warning("readBufferSize must be bytes format")).WithCause(err).WithMeta("fns", "http")
			return
		}
	}
	readTimeout := 10 * time.Second
	if opt.ReadTimeout != "" {
		readTimeout, err = time.ParseDuration(strings.TrimSpace(opt.ReadTimeout))
		if err != nil {
			err = errors.Warning("fns: build server failed").WithCause(errors.Warning("readTimeout must be time.Duration format")).WithCause(err).WithMeta("fns", "http")
			return
		}
	}
	writeBufferSize := uint64(0)
	if opt.WriteBufferSize != "" {
		writeBufferSize, err = bytex.ToBytes(strings.TrimSpace(opt.WriteBufferSize))
		if err != nil {
			err = errors.Warning("fns: build server failed").WithCause(errors.Warning("writeBufferSize must be bytes format")).WithCause(err).WithMeta("fns", "http")
			return
		}
	}
	writeTimeout := 10 * time.Second
	if opt.WriteTimeout != "" {
		writeTimeout, err = time.ParseDuration(strings.TrimSpace(opt.WriteTimeout))
		if err != nil {
			err = errors.Warning("fns: build server failed").WithCause(errors.Warning("writeTimeout must be time.Duration format")).WithCause(err).WithMeta("fns", "http")
			return
		}
	}
	maxIdleWorkerDuration := time.Duration(0)
	if opt.MaxIdleWorkerDuration != "" {
		maxIdleWorkerDuration, err = time.ParseDuration(strings.TrimSpace(opt.MaxIdleWorkerDuration))
		if err != nil {
			err = errors.Warning("fns: build server failed").WithCause(errors.Warning("maxIdleWorkerDuration must be time.Duration format")).WithCause(err).WithMeta("fns", "http")
			return
		}
	}
	tcpKeepalivePeriod := time.Duration(0)
	if opt.TCPKeepalivePeriod != "" {
		tcpKeepalivePeriod, err = time.ParseDuration(strings.TrimSpace(opt.TCPKeepalivePeriod))
		if err != nil {
			err = errors.Warning("fns: build server failed").WithCause(errors.Warning("tcpKeepalivePeriod must be time.Duration format")).WithCause(err).WithMeta("fns", "http")
			return
		}
	}

	maxRequestBodySize := uint64(4 * bytex.MEGABYTE)
	if opt.MaxRequestBodySize != "" {
		maxRequestBodySize, err = bytex.ToBytes(strings.TrimSpace(opt.MaxRequestBodySize))
		if err != nil {
			err = errors.Warning("fns: build server failed").WithCause(errors.Warning("maxRequestBodySize must be bytes format")).WithCause(err).WithMeta("fns", "http")
			return
		}
	}
	reduceMemoryUsage := opt.ReduceMemoryUsage
	// server
	srv.fast = &fasthttp.Server{
		Handler:                            fasthttpadaptor.NewFastHTTPHandler(options.Handler),
		ErrorHandler:                       fastHttpErrorHandler,
		ReadBufferSize:                     int(readBufferSize),
		WriteBufferSize:                    int(writeBufferSize),
		ReadTimeout:                        readTimeout,
		WriteTimeout:                       writeTimeout,
		MaxIdleWorkerDuration:              maxIdleWorkerDuration,
		TCPKeepalivePeriod:                 tcpKeepalivePeriod,
		MaxRequestBodySize:                 int(maxRequestBodySize),
		TCPKeepalive:                       opt.TCPKeepalive,
		ReduceMemoryUsage:                  reduceMemoryUsage,
		SleepWhenConcurrencyLimitsExceeded: 10 * time.Second,
		NoDefaultServerHeader:              true,
		KeepHijackedConns:                  opt.KeepHijackedConns,
		CloseOnShutdown:                    true,
		StreamRequestBody:                  opt.StreamRequestBody,
		ConnState:                          nil,
		Logger:                             logs.MapToLogger(options.Log, logs.DebugLevel, false),
		TLSConfig:                          options.ServerTLS,
		FormValueFunc:                      nil,
	}
	http2.ConfigureServer(srv.fast, http2.ServerConfig{})
	// dialer
	dialer, dialerErr := NewDialer(options.ClientTLS, &opt.Client)
	if dialerErr != nil {
		err = errors.Warning("http2: build server failed").WithCause(dialerErr)
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
	err = srv.fast.ServeTLS(srv.ln, "", "")
	if err != nil {
		err = errors.Warning("http2: listen and serve failed").WithCause(err)
		return
	}
	return
}

func (srv *server) Close() (err error) {
	srv.dialer.Close()
	err = srv.ln.Close()
	if err != nil {
		err = errors.Warning("http2: close failed").WithCause(err)
		return
	}
	return
}

func fastHttpErrorHandler(ctx *fasthttp.RequestCtx, err error) {
	ctx.SetStatusCode(555)
	ctx.SetContentType(httpContentTypeJson)
	ctx.SetBody([]byte(fmt.Sprintf("{\"error\": \"%s\"}", err.Error())))
}
