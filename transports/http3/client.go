package http3

import (
	"context"
	"fmt"
	"github.com/aacfactory/errors"
	"github.com/aacfactory/fns/commons/bytex"
	"github.com/aacfactory/fns/service/transports"
	"github.com/quic-go/quic-go/http3"
	"github.com/valyala/bytebufferpool"
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

func (c Client) Do(ctx context.Context, request *transports.Request) (response *transports.Response, err error) {
	r, rErr := http.NewRequestWithContext(ctx, bytex.ToString(request.Method()), fmt.Sprintf("https://%s%s", c.address, bytex.ToString(request.Path())), nil)
	if rErr != nil {
		err = errors.Warning("http3: create request failed").WithCause(rErr)
		return
	}
	if request.Header() != nil && len(request.Header()) > 0 {
		r.Header = http.Header(request.Header())
	}
	resp, doErr := c.core.Do(r)
	if doErr != nil {
		if errors.Map(doErr).Contains(context.Canceled) || errors.Map(doErr).Contains(context.DeadlineExceeded) {
			err = errors.Timeout("http3: do failed").WithCause(doErr)
			return
		}
		err = errors.Warning("http3: do failed").WithCause(doErr)
		return
	}
	buf := acquireBuf()
	defer releaseBuf(buf)
	b := bytebufferpool.Get()
	defer bytebufferpool.Put(b)
	for {
		n, readErr := resp.Body.Read(buf)
		_, _ = b.Write(buf[0:n])
		if readErr != nil {
			if readErr == io.EOF {
				break
			}
			_ = resp.Body.Close()
			err = errors.Warning("http3: do failed").WithCause(errors.Warning("read response body failed").WithCause(readErr))
			return
		}
	}
	response = &transports.Response{
		Status: resp.StatusCode,
		Header: transports.Header(resp.Header),
		Body:   b.Bytes(),
	}
	return
}

func (c Client) Close() {
	c.core.CloseIdleConnections()
	_ = c.roundTripper.Close()
	return
}
