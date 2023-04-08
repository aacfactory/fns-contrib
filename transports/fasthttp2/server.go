package fasthttp2

import (
	"bytes"
	"fmt"
	"github.com/aacfactory/errors"
	"github.com/aacfactory/fns/commons/bytex"
	"github.com/aacfactory/fns/service/transports"
	"github.com/aacfactory/json"
	"github.com/aacfactory/logs"
	"github.com/dgrr/http2"
	"github.com/valyala/fasthttp"
	"io"
	"net"
	"net/http"
	"net/url"
	"strings"
	"time"
)

const (
	fastHttp2TransportName = "fasthttp2"
)

func Server() transports.Transport {
	return &server{}
}

type server struct {
	log    logs.Logger
	fast   *fasthttp.Server
	ln     net.Listener
	dialer *Dialer
}

func (srv *server) Name() (name string) {
	name = fastHttp2TransportName
	return
}

func (srv *server) Build(options transports.Options) (err error) {
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
	opt := transports.FastHttpTransportOptions{}
	optErr := options.Config.As(&opt)
	if optErr != nil {
		err = errors.Warning("http2: build failed").WithCause(optErr)
		return
	}

	readBufferSize := uint64(0)
	if opt.ReadBufferSize != "" {
		readBufferSize, err = bytex.ParseBytes(strings.TrimSpace(opt.ReadBufferSize))
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
		writeBufferSize, err = bytex.ParseBytes(strings.TrimSpace(opt.WriteBufferSize))
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
		maxRequestBodySize, err = bytex.ParseBytes(strings.TrimSpace(opt.MaxRequestBodySize))
		if err != nil {
			err = errors.Warning("fns: build server failed").WithCause(errors.Warning("maxRequestBodySize must be bytes format")).WithCause(err).WithMeta("fns", "http")
			return
		}
	}
	reduceMemoryUsage := opt.ReduceMemoryUsage
	// server
	srv.fast = &fasthttp.Server{
		Handler:                            transports.FastHttpTransportHandlerAdaptor(options.Handler),
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

func (srv *server) Dial(address string) (client transports.Client, err error) {
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

func fastHttpTransportHandlerAdaptor(h http.Handler) fasthttp.RequestHandler {
	return func(ctx *fasthttp.RequestCtx) {
		var r http.Request
		if err := convertFastHttpRequestToHttpRequest(ctx, &r, true); err != nil {
			p, _ := json.Marshal(errors.Warning("fns: cannot parse requestURI").WithMeta("uri", r.RequestURI).WithMeta("transport", fastHttp2TransportName).WithCause(err))
			ctx.Response.Reset()
			ctx.SetStatusCode(555)
			ctx.SetContentTypeBytes(bytex.FromString(httpContentTypeJson))
			ctx.SetBody(p)
			return
		}

		w := netHTTPResponseWriter{w: ctx.Response.BodyWriter()}
		h.ServeHTTP(&w, r.WithContext(ctx))

		ctx.SetStatusCode(w.StatusCode())
		haveContentType := false
		for k, vv := range w.Header() {
			if k == fasthttp.HeaderContentType {
				haveContentType = true
			}

			for _, v := range vv {
				ctx.Response.Header.Add(k, v)
			}
		}
		if !haveContentType {
			l := 512
			b := ctx.Response.Body()
			if len(b) < 512 {
				l = len(b)
			}
			ctx.Response.Header.Set(fasthttp.HeaderContentType, http.DetectContentType(b[:l]))
		}
	}
}

func convertFastHttpRequestToHttpRequest(ctx *fasthttp.RequestCtx, r *http.Request, forServer bool) error {
	body := ctx.PostBody()
	strRequestURI := bytex.ToString(ctx.RequestURI())

	rURL, err := url.ParseRequestURI(strRequestURI)
	if err != nil {
		return err
	}

	r.Method = bytex.ToString(ctx.Method())
	r.Proto = bytex.ToString(ctx.Request.Header.Protocol())
	if r.Proto == "HTTP/2" {
		r.ProtoMajor = 2
	} else {
		r.ProtoMajor = 1
	}
	r.ProtoMinor = 1
	r.ContentLength = int64(len(body))
	r.RemoteAddr = ctx.RemoteAddr().String()
	r.Host = bytex.ToString(ctx.Host())
	r.TLS = ctx.TLSConnectionState()
	r.Body = io.NopCloser(bytes.NewReader(body))
	r.URL = rURL

	if forServer {
		r.RequestURI = strRequestURI
	}

	if r.Header == nil {
		r.Header = make(http.Header)
	} else if len(r.Header) > 0 {
		for k := range r.Header {
			delete(r.Header, k)
		}
	}

	ctx.Request.Header.VisitAll(func(k, v []byte) {
		sk := bytex.ToString(k)
		sv := bytex.ToString(v)

		switch sk {
		case "Transfer-Encoding":
			r.TransferEncoding = append(r.TransferEncoding, sv)
		default:
			r.Header.Set(sk, sv)
		}
	})

	return nil
}

type netHTTPResponseWriter struct {
	statusCode int
	h          http.Header
	w          io.Writer
}

func (w *netHTTPResponseWriter) StatusCode() int {
	if w.statusCode == 0 {
		return http.StatusOK
	}
	return w.statusCode
}

func (w *netHTTPResponseWriter) Header() http.Header {
	if w.h == nil {
		w.h = make(http.Header)
	}
	return w.h
}

func (w *netHTTPResponseWriter) WriteHeader(statusCode int) {
	w.statusCode = statusCode
}

func (w *netHTTPResponseWriter) Write(p []byte) (int, error) {
	return w.w.Write(p)
}

func (w *netHTTPResponseWriter) Flush() {}
