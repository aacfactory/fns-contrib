package http2

import (
	"context"
	"crypto/tls"
	"github.com/aacfactory/errors"
	"github.com/aacfactory/fns/commons/bytex"
	"github.com/aacfactory/fns/service/transports"
	"github.com/dgrr/http2"
	"github.com/valyala/fasthttp"
	"strings"
	"time"
)

const (
	httpContentType     = "Content-Type"
	httpContentTypeJson = "application/json"
)

func NewClientOptions(opt *transports.FastHttpClientOptions) (v *ClientOptions, err error) {
	if opt == nil {
		opt = &transports.FastHttpClientOptions{
			DialDualStack:             false,
			MaxConnsPerHost:           0,
			MaxIdleConnDuration:       "",
			MaxConnDuration:           "",
			MaxIdemponentCallAttempts: 0,
			ReadBufferSize:            "4K",
			ReadTimeout:               "",
			WriteBufferSize:           "4K",
			WriteTimeout:              "",
			MaxResponseBodySize:       "",
			MaxConnWaitTimeout:        "",
		}
	}
	maxIdleWorkerDuration := time.Duration(0)
	if opt.MaxIdleConnDuration != "" {
		maxIdleWorkerDuration, err = time.ParseDuration(strings.TrimSpace(opt.MaxIdleConnDuration))
		if err != nil {
			err = errors.Warning("fns: build client failed").WithCause(errors.Warning("maxIdleWorkerDuration must be time.Duration format")).WithCause(err).WithMeta("fns", "http")
			return
		}
	}
	maxConnDuration := time.Duration(0)
	if opt.MaxConnDuration != "" {
		maxConnDuration, err = time.ParseDuration(strings.TrimSpace(opt.MaxConnDuration))
		if err != nil {
			err = errors.Warning("fns: build client failed").WithCause(errors.Warning("maxConnDuration must be time.Duration format")).WithCause(err).WithMeta("fns", "http")
			return
		}
	}
	readBufferSize := uint64(0)
	if opt.ReadBufferSize != "" {
		readBufferSize, err = bytex.ParseBytes(strings.TrimSpace(opt.ReadBufferSize))
		if err != nil {
			err = errors.Warning("fns: build client failed").WithCause(errors.Warning("readBufferSize must be bytes format")).WithCause(err).WithMeta("fns", "http")
			return
		}
	}
	readTimeout := 10 * time.Second
	if opt.ReadTimeout != "" {
		readTimeout, err = time.ParseDuration(strings.TrimSpace(opt.ReadTimeout))
		if err != nil {
			err = errors.Warning("fns: build client failed").WithCause(errors.Warning("readTimeout must be time.Duration format")).WithCause(err).WithMeta("fns", "http")
			return
		}
	}
	writeBufferSize := uint64(0)
	if opt.WriteBufferSize != "" {
		writeBufferSize, err = bytex.ParseBytes(strings.TrimSpace(opt.WriteBufferSize))
		if err != nil {
			err = errors.Warning("fns: build client failed").WithCause(errors.Warning("writeBufferSize must be bytes format")).WithCause(err).WithMeta("fns", "http")
			return
		}
	}
	writeTimeout := 10 * time.Second
	if opt.WriteTimeout != "" {
		writeTimeout, err = time.ParseDuration(strings.TrimSpace(opt.WriteTimeout))
		if err != nil {
			err = errors.Warning("fns: build client failed").WithCause(errors.Warning("writeTimeout must be time.Duration format")).WithCause(err).WithMeta("fns", "http")
			return
		}
	}
	maxResponseBodySize := uint64(4 * bytex.MEGABYTE)
	if opt.MaxResponseBodySize != "" {
		maxResponseBodySize, err = bytex.ParseBytes(strings.TrimSpace(opt.MaxResponseBodySize))
		if err != nil {
			err = errors.Warning("fns: build client failed").WithCause(errors.Warning("maxResponseBodySize must be bytes format")).WithCause(err).WithMeta("fns", "http")
			return
		}
	}
	maxConnWaitTimeout := time.Duration(0)
	if opt.MaxConnWaitTimeout != "" {
		maxConnWaitTimeout, err = time.ParseDuration(strings.TrimSpace(opt.MaxConnWaitTimeout))
		if err != nil {
			err = errors.Warning("fns: build client failed").WithCause(errors.Warning("maxConnWaitTimeout must be time.Duration format")).WithCause(err).WithMeta("fns", "http")
			return
		}
	}
	v = &ClientOptions{
		MaxConns:                  opt.MaxConnsPerHost,
		MaxConnDuration:           maxConnDuration,
		MaxIdleConnDuration:       maxIdleWorkerDuration,
		MaxIdemponentCallAttempts: opt.MaxIdemponentCallAttempts,
		ReadBufferSize:            int(readBufferSize),
		WriteBufferSize:           int(writeBufferSize),
		ReadTimeout:               readTimeout,
		WriteTimeout:              writeTimeout,
		MaxResponseBodySize:       int(maxResponseBodySize),
		MaxConnWaitTimeout:        maxConnWaitTimeout,
	}
	return
}

type ClientOptions struct {
	MaxConns                  int
	MaxConnDuration           time.Duration
	MaxIdleConnDuration       time.Duration
	MaxIdemponentCallAttempts int
	ReadBufferSize            int
	WriteBufferSize           int
	ReadTimeout               time.Duration
	WriteTimeout              time.Duration
	MaxResponseBodySize       int
	MaxConnWaitTimeout        time.Duration
}

func NewClient(address string, cliTLS *tls.Config, opts *ClientOptions) (client *Client, err error) {
	hc := &fasthttp.HostClient{
		Addr:                          address,
		Name:                          "",
		NoDefaultUserAgentHeader:      true,
		IsTLS:                         true,
		TLSConfig:                     cliTLS,
		MaxConns:                      opts.MaxConns,
		MaxConnDuration:               opts.MaxConnDuration,
		MaxIdleConnDuration:           opts.MaxIdleConnDuration,
		MaxIdemponentCallAttempts:     opts.MaxIdemponentCallAttempts,
		ReadBufferSize:                opts.ReadBufferSize,
		WriteBufferSize:               opts.WriteBufferSize,
		ReadTimeout:                   opts.ReadTimeout,
		WriteTimeout:                  opts.WriteTimeout,
		MaxResponseBodySize:           opts.MaxResponseBodySize,
		DisableHeaderNamesNormalizing: false,
		DisablePathNormalizing:        false,
		SecureErrorLogMessage:         false,
		MaxConnWaitTimeout:            opts.MaxConnWaitTimeout,
		RetryIf:                       nil,
		Transport:                     nil,
		ConnPoolStrategy:              fasthttp.FIFO,
	}
	configErr := http2.ConfigureClient(hc, http2.ClientOpts{
		PingInterval:    0,
		MaxResponseTime: 10 * time.Second,
		OnRTT:           nil,
	})
	if configErr != nil {
		err = errors.Warning("http2: configure client failed").WithCause(configErr)
		return
	}
	client = &Client{
		address: address,
		core:    hc,
	}
	return
}

type Client struct {
	address string
	core    *fasthttp.HostClient
}

func (client *Client) Do(ctx context.Context, request *transports.Request) (response *transports.Response, err error) {
	req := fasthttp.AcquireRequest()
	// method
	req.Header.SetMethodBytes(request.Method())
	// header
	if request.Header() != nil && len(request.Header()) > 0 {
		for k, vv := range request.Header() {
			if vv == nil || len(vv) == 0 {
				continue
			}
			for _, v := range vv {
				req.Header.Add(k, v)
			}
		}
	}
	// uri
	uri := req.URI()
	uri.SetSchemeBytes(bytex.FromString("https"))
	uri.SetHostBytes(bytex.FromString(client.address))
	uri.SetPathBytes(request.Path())
	if request.Params() != nil && len(request.Params()) > 0 {
		uri.SetQueryStringBytes(bytex.FromString(request.Params().String()))
	}
	// body
	if request.Body() != nil && len(request.Body()) > 0 {
		req.SetBodyRaw(request.Body())
	}
	// resp
	resp := fasthttp.AcquireResponse()
	// do
	deadline, hasDeadline := ctx.Deadline()
	if hasDeadline {
		err = client.core.DoDeadline(req, resp, deadline)
	} else {
		err = client.core.Do(req, resp)
	}
	if err != nil {
		err = errors.Warning("fns: transport client do failed").
			WithCause(err).
			WithMeta("transport", fastHttp2TransportName).WithMeta("method", bytex.ToString(request.Method())).WithMeta("path", bytex.ToString(request.Path()))
		fasthttp.ReleaseRequest(req)
		fasthttp.ReleaseResponse(resp)
		return
	}
	response = &transports.Response{
		Status: resp.StatusCode(),
		Header: make(transports.Header),
		Body:   resp.Body(),
	}
	resp.Header.VisitAll(func(key, value []byte) {
		response.Header.Add(bytex.ToString(key), bytex.ToString(value))
	})
	fasthttp.ReleaseRequest(req)
	fasthttp.ReleaseResponse(resp)
	return
}

func (client *Client) Key() (key string) {
	key = client.address
	return
}

func (client *Client) Close() {
	client.core.CloseIdleConnections()
}
