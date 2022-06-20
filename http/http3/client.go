package http3

import (
	"bytes"
	"context"
	"fmt"
	"github.com/aacfactory/errors"
	"github.com/aacfactory/fns/cluster"
	"github.com/aacfactory/logs"
	"github.com/lucas-clemente/quic-go/http3"
	"io/ioutil"
	"net/http"
	"time"
)

func ClientBuild(options cluster.ClientOptions) (client cluster.Client, err error) {
	timeout := options.RequestTimeout
	if timeout == 0 {
		timeout = 10 * time.Second
	}
	rt := &http3.RoundTripper{}
	if options.TLS != nil {
		rt.TLSClientConfig = options.TLS
	}
	c := &http.Client{
		Transport: rt,
		Timeout:   timeout,
	}
	client = &Client{
		log:    options.Log,
		client: c,
	}
	return
}

type Client struct {
	log    logs.Logger
	client *http.Client
}

func (client *Client) Do(_ context.Context, method string, address string, uri string, header http.Header, body []byte) (status int, respHeader http.Header, respBody []byte, err error) {
	req, reqErr := http.NewRequest(method, fmt.Sprintf("https://%s%s", address, uri), bytes.NewReader(body))
	if reqErr != nil {
		err = errors.Warning("fns: create proxy request failed").WithCause(reqErr).WithMeta("method", method).WithMeta("address", address).WithMeta("uri", uri)
		return
	}
	if header != nil {
		req.Header = header
	}
	resp, doErr := client.client.Do(req)
	if doErr != nil {
		err = errors.Warning("fns: do proxy request failed").WithCause(doErr).WithMeta("method", method).WithMeta("address", address).WithMeta("uri", uri)
		return
	}
	status = resp.StatusCode
	respHeader = resp.Header
	respBody, err = ioutil.ReadAll(resp.Body)
	_ = resp.Body.Close()
	if err != nil {
		err = errors.Warning("fns: read proxy response body failed").WithCause(err).WithMeta("method", method).WithMeta("address", address).WithMeta("uri", uri)
		return
	}
	return
}

func (client *Client) Close() {
	client.client.CloseIdleConnections()
}
