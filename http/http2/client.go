package http2

import (
	"context"
	"crypto/tls"
	"github.com/aacfactory/errors"
	"github.com/aacfactory/fns/commons/bytex"
	"github.com/aacfactory/fns/service"
	"github.com/dgrr/http2"
	"github.com/valyala/fasthttp"
	"net/http"
	"strings"
	"time"
)

const (
	httpContentType     = "Content-Type"
	httpContentTypeJson = "application/json"
)

func NewClientOptions(opt *service.FastHttpClientOptions) (v *ClientOptions, err error) {
	if opt == nil {
		opt = &service.FastHttpClientOptions{
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
		readBufferSize, err = bytex.ToBytes(strings.TrimSpace(opt.ReadBufferSize))
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
		writeBufferSize, err = bytex.ToBytes(strings.TrimSpace(opt.WriteBufferSize))
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
		maxResponseBodySize, err = bytex.ToBytes(strings.TrimSpace(opt.MaxResponseBodySize))
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

func (client *Client) Key() (key string) {
	key = client.address
	return
}

func (client *Client) Get(ctx context.Context, path string, header http.Header) (status int, respHeader http.Header, respBody []byte, err error) {
	req := client.prepareRequest(bytex.FromString(http.MethodGet), path, header)
	resp := fasthttp.AcquireResponse()
	deadline, hasDeadline := ctx.Deadline()
	if hasDeadline {
		err = client.core.DoDeadline(req, resp, deadline)
	} else {
		err = client.core.Do(req, resp)
	}
	if err != nil {
		err = errors.Warning("fns: fasthttp client do get failed").WithCause(err)
		fasthttp.ReleaseRequest(req)
		fasthttp.ReleaseResponse(resp)
		return
	}
	status = resp.StatusCode()
	respHeader = http.Header{}
	resp.Header.VisitAll(func(key, value []byte) {
		respHeader.Add(bytex.ToString(key), bytex.ToString(value))
	})
	respBody = resp.Body()
	fasthttp.ReleaseRequest(req)
	fasthttp.ReleaseResponse(resp)
	return
}

func (client *Client) Post(ctx context.Context, path string, header http.Header, body []byte) (status int, respHeader http.Header, respBody []byte, err error) {
	req := client.prepareRequest(bytex.FromString(http.MethodPost), path, header)
	if body != nil && len(body) > 0 {
		req.SetBodyRaw(body)
	}
	resp := fasthttp.AcquireResponse()
	deadline, hasDeadline := ctx.Deadline()
	if hasDeadline {
		err = client.core.DoDeadline(req, resp, deadline)
	} else {
		err = client.core.Do(req, resp)
	}
	if err != nil {
		err = errors.Warning("fns: fasthttp client do post failed").WithCause(err)
		fasthttp.ReleaseRequest(req)
		fasthttp.ReleaseResponse(resp)
		return
	}
	status = resp.StatusCode()
	respHeader = http.Header{}
	resp.Header.VisitAll(func(key, value []byte) {
		respHeader.Add(bytex.ToString(key), bytex.ToString(value))
	})
	respBody = resp.Body()
	fasthttp.ReleaseRequest(req)
	fasthttp.ReleaseResponse(resp)
	return
}

func (client *Client) Close() {
	client.core.CloseIdleConnections()
}

func (client *Client) prepareRequest(method []byte, path string, header http.Header) (req *fasthttp.Request) {
	req = fasthttp.AcquireRequest()
	uri := req.URI()
	uri.SetSchemeBytes(bytex.FromString("https"))
	uri.SetPathBytes(bytex.FromString(path))
	uri.SetHostBytes(bytex.FromString(client.address))
	req.Header.SetMethodBytes(method)
	if header != nil && len(header) > 0 {
		for k, vv := range header {
			for _, v := range vv {
				req.Header.Add(k, v)
			}
		}
	}
	req.Header.SetBytesKV(bytex.FromString(httpContentType), bytex.FromString(httpContentTypeJson))
	return
}
