package http2

import (
	"fmt"
	"github.com/aacfactory/errors"
	"github.com/aacfactory/fns/commons/bytex"
	"github.com/aacfactory/fns/service"
	"github.com/aacfactory/json"
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
	opt := &service.FastHttpOptions{}
	if options.Options != nil && len(options.Options) > 2 {
		optErr := json.Unmarshal(options.Options, opt)
		if optErr != nil {
			err = errors.Warning("http2: build failed").WithCause(optErr)
			return
		}
	}
	if opt.ReadTimeoutSeconds < 1 {
		opt.ReadTimeoutSeconds = 2
	}
	if opt.MaxWorkerIdleSeconds < 1 {
		opt.MaxWorkerIdleSeconds = 10
	}
	maxRequestBody := strings.ToUpper(strings.TrimSpace(opt.MaxRequestBodySize))
	if maxRequestBody == "" {
		maxRequestBody = "4MB"
	}
	maxRequestBodySize, maxRequestBodySizeErr := bytex.ToBytes(maxRequestBody)
	if maxRequestBodySizeErr != nil {
		err = errors.Warning("http2: build server failed").WithCause(maxRequestBodySizeErr)
		return
	}
	reduceMemoryUsage := opt.ReduceMemoryUsage
	// server
	srv.fast = &fasthttp.Server{
		Handler:                            fasthttpadaptor.NewFastHTTPHandler(options.Handler),
		TLSConfig:                          options.ServerTLS,
		ErrorHandler:                       fastHttpErrorHandler,
		ReadTimeout:                        time.Duration(opt.ReadTimeoutSeconds) * time.Second,
		MaxIdleWorkerDuration:              time.Duration(opt.MaxWorkerIdleSeconds) * time.Second,
		MaxRequestBodySize:                 int(maxRequestBodySize),
		ReduceMemoryUsage:                  reduceMemoryUsage,
		DisablePreParseMultipartForm:       true,
		SleepWhenConcurrencyLimitsExceeded: 10 * time.Second,
		NoDefaultServerHeader:              true,
		NoDefaultDate:                      false,
		NoDefaultContentType:               false,
		CloseOnShutdown:                    true,
		Logger:                             logs.MapToLogger(options.Log, logs.DebugLevel, false),
	}
	http2.ConfigureServer(srv.fast, http2.ServerConfig{})
	// dialer
	dialer, dialerErr := NewDialer(options.ClientTLS, opt.Client)
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
