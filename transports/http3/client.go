package http3

import (
	"fmt"
	"github.com/aacfactory/errors"
	"github.com/aacfactory/fns/commons/bytex"
	"github.com/aacfactory/fns/context"
	"github.com/aacfactory/fns/transports"
	"github.com/aacfactory/fns/transports/standard"
	"github.com/quic-go/quic-go/http3"
	"io"
	"net/http"
	"time"
)

func NewClient(address string, roundTripper *http3.RoundTripper, timeout time.Duration) (client *Client) {
	client = &Client{
		address:      address,
		roundTripper: roundTripper,
		core: &http.Client{
			Transport:     roundTripper,
			CheckRedirect: nil,
			Jar:           nil,
			Timeout:       timeout,
		},
	}
	return
}

type Client struct {
	address      string
	roundTripper *http3.RoundTripper
	core         *http.Client
}

func (c *Client) Key() (key string) {
	key = c.address
	return
}

func (c *Client) Do(ctx context.Context, method []byte, path []byte, header transports.Header, body []byte) (status int, responseHeader transports.Header, responseBody []byte, err error) {
	url := fmt.Sprintf("https://%s%s", c.address, bytex.ToString(path))
	rb := bytex.AcquireBuffer()
	defer bytex.ReleaseBuffer(rb)
	_, _ = rb.Write(body)
	r, rErr := http.NewRequestWithContext(ctx, bytex.ToString(method), url, rb)
	if rErr != nil {
		err = errors.Warning("http3: create request failed").WithCause(rErr)
		return
	}
	if header != nil && header.Len() > 0 {
		header.Foreach(func(key []byte, values [][]byte) {
			for _, value := range values {
				r.Header.Add(bytex.ToString(key), bytex.ToString(value))
			}
		})
	}
	resp, doErr := c.core.Do(r)
	if doErr != nil {
		if errors.Wrap(doErr).Contains(context.Canceled) || errors.Wrap(doErr).Contains(context.DeadlineExceeded) {
			err = errors.Timeout("http3: do failed").WithCause(doErr)
			return
		}
		err = errors.Warning("http3: do failed").WithCause(doErr)
		return
	}
	buf := bytex.Acquire4KBuffer()
	defer bytex.Release4KBuffer(buf)
	responseBody = make([]byte, 0, 1024)
	for {
		n, readErr := resp.Body.Read(buf)
		if n > 0 {
			body = append(body, buf[0:n]...)
		}
		if readErr != nil {
			if readErr == io.EOF {
				break
			}
			_ = resp.Body.Close()
			err = errors.Warning("http3: do failed").WithCause(errors.Warning("read response body failed").WithCause(readErr))
			return
		}
	}
	status = resp.StatusCode
	responseHeader = standard.WrapHttpHeader(resp.Header)
	return
}

func (c *Client) Close() {
	c.core.CloseIdleConnections()
	_ = c.roundTripper.Close()
	return
}
