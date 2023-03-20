package http3

import (
	"bytes"
	"context"
	"fmt"
	"github.com/aacfactory/errors"
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

func (c Client) Key() (key string) {
	key = c.address
	return
}

func (c Client) Get(ctx context.Context, path string, header http.Header) (status int, respHeader http.Header, respBody []byte, err error) {
	r, rErr := http.NewRequestWithContext(ctx, http.MethodGet, fmt.Sprintf("https://%s%s", c.address, path), nil)
	if rErr != nil {
		err = errors.Warning("http3: create request failed").WithCause(rErr)
		return
	}
	if header != nil && len(header) > 0 {
		r.Header = header
	}
	resp, doErr := c.core.Do(r)
	if doErr != nil {
		if errors.Map(doErr).Contains(context.Canceled) || errors.Map(doErr).Contains(context.DeadlineExceeded) {
			err = errors.Timeout("http3: get failed").WithCause(doErr)
			return
		}
		err = errors.Warning("http3: get failed").WithCause(doErr)
		return
	}
	status = resp.StatusCode
	respHeader = resp.Header
	respBody, err = io.ReadAll(resp.Body)
	if err != nil {
		err = errors.Warning("http3: get failed").WithCause(err)
		return
	}
	_ = resp.Body.Close()
	return
}

func (c Client) Post(ctx context.Context, path string, header http.Header, body []byte) (status int, respHeader http.Header, respBody []byte, err error) {
	if body == nil || len(body) == 0 {
		body = []byte{'{', '}'}
	}
	r, rErr := http.NewRequestWithContext(ctx, http.MethodPost, fmt.Sprintf("https://%s%s", c.address, path), bytes.NewBuffer(body))
	if rErr != nil {
		err = errors.Warning("http3: create request failed").WithCause(rErr)
		return
	}
	if header != nil && len(header) > 0 {
		r.Header = header
	}
	resp, doErr := c.core.Do(r)
	if doErr != nil {
		if errors.Map(doErr).Contains(context.Canceled) || errors.Map(doErr).Contains(context.DeadlineExceeded) {
			err = errors.Timeout("http3: post failed").WithCause(doErr)
			return
		}
		err = errors.Warning("http3: post failed").WithCause(doErr)
		return
	}
	status = resp.StatusCode
	respHeader = resp.Header
	respBody, err = io.ReadAll(resp.Body)
	if err != nil {
		err = errors.Warning("http3: post failed").WithCause(err)
		return
	}
	_ = resp.Body.Close()
	return
}

func (c Client) Close() {
	c.core.CloseIdleConnections()
	_ = c.roundTripper.Close()
	return
}
