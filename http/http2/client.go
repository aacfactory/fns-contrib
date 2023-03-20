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

func NewClientOptions(opts *service.FastHttpClientOptions) (v *ClientOptions, err error) {
	if opts == nil {
		opts = &service.FastHttpClientOptions{
			DialDualStack:             false,
			MaxConnsPerHost:           0,
			MaxIdleConnSeconds:        0,
			MaxConnSeconds:            0,
			MaxIdemponentCallAttempts: 0,
			ReadBufferSize:            "4MB",
			WriteBufferSize:           "4MB",
			ReadTimeoutSeconds:        0,
			WriteTimeoutSeconds:       0,
			MaxResponseBodySize:       "4MB",
			MaxConnWaitTimeoutSeconds: 0,
		}
	}
	readBufferSize := strings.ToUpper(strings.TrimSpace(opts.ReadBufferSize))
	if readBufferSize == "" {
		readBufferSize = "4MB"
	}
	readBuffer, readBufferErr := bytex.ToBytes(readBufferSize)
	if readBufferErr != nil {
		err = errors.Warning("fns: build server failed").WithCause(readBufferErr).WithMeta("fns", "http")
		return
	}

	writeBufferSize := strings.ToUpper(strings.TrimSpace(opts.WriteBufferSize))
	if writeBufferSize == "" {
		writeBufferSize = "4MB"
	}
	writeBuffer, writeBufferErr := bytex.ToBytes(writeBufferSize)
	if writeBufferErr != nil {
		err = errors.Warning("fns: build server failed").WithCause(writeBufferErr).WithMeta("fns", "http")
		return
	}

	maxResponseBodySize := strings.ToUpper(strings.TrimSpace(opts.MaxResponseBodySize))
	if maxResponseBodySize == "" {
		maxResponseBodySize = "4MB"
	}
	maxResponseBody, maxResponseBodyErr := bytex.ToBytes(maxResponseBodySize)
	if maxResponseBodyErr != nil {
		err = errors.Warning("fns: build server failed").WithCause(maxResponseBodyErr).WithMeta("fns", "http")
		return
	}
	v = &ClientOptions{
		MaxConns:                  opts.MaxConnsPerHost,
		MaxConnDuration:           time.Duration(opts.MaxConnSeconds) * time.Second,
		MaxIdleConnDuration:       time.Duration(opts.MaxIdleConnSeconds) * time.Second,
		MaxIdemponentCallAttempts: opts.MaxIdemponentCallAttempts,
		ReadBufferSize:            int(readBuffer),
		WriteBufferSize:           int(writeBuffer),
		ReadTimeout:               time.Duration(opts.ReadTimeoutSeconds) * time.Second,
		WriteTimeout:              time.Duration(opts.WriteTimeoutSeconds) * time.Second,
		MaxResponseBodySize:       int(maxResponseBody),
		MaxConnWaitTimeout:        time.Duration(opts.MaxConnWaitTimeoutSeconds) * time.Second,
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
