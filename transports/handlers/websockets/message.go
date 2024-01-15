package websockets

import (
	"fmt"
	"github.com/aacfactory/errors"
	"github.com/aacfactory/fns/commons/bytex"
	"github.com/aacfactory/fns/commons/versions"
	"github.com/aacfactory/fns/transports"
	"github.com/aacfactory/json"
	"io"
	"sync"
)

var (
	requestPool = sync.Pool{}
)

func AcquireRequest(endpoint []byte, fn []byte, payload any) (r *Request, err error) {
	if len(endpoint) == 0 || len(fn) == 0 {
		err = errors.Warning("websockets: acquire request failed").WithCause(errors.Warning("endpoint and fn is required"))
		return
	}
	var p []byte
	if payload == nil {
		p = []byte{'{', '}'}
	} else {
		p, err = json.Marshal(payload)
		if err != nil {
			err = errors.Warning("websockets: acquire request failed").WithCause(errors.Warning("encode payload failed").WithCause(err))
			return
		}
	}
	cr := requestPool.Get()
	if cr == nil {
		cr = new(Request)
	}
	r = cr.(*Request)
	r.Endpoint = bytex.ToString(endpoint)
	r.Fn = bytex.ToString(fn)
	r.Header = transports.AcquireHeader()
	r.Payload = p
	return
}

func ReleaseRequest(r *Request) {
	header := r.Header
	transports.ReleaseHeader(header)
	r.Header = nil
	r.Endpoint = ""
	r.Fn = ""
	r.Payload = nil
	requestPool.Put(r)
}

type Request struct {
	Endpoint string            `json:"endpoint"`
	Fn       string            `json:"fn"`
	Header   transports.Header `json:"header"`
	Payload  json.RawMessage   `json:"payload"`
}

func (r *Request) Validate() (err error) {
	if len(r.Endpoint) == 0 || len(r.Fn) == 0 {
		err = errors.Warning("websocket: invalid request")
		return
	}
	return
}

func (r *Request) Authorization() (v []byte) {
	return r.Header.Get(transports.AuthorizationHeaderName)
}

func (r *Request) Versions() (v versions.Intervals, has bool, err error) {
	rv := r.Header.Get(transports.RequestVersionsHeaderName)
	if len(rv) == 0 {
		return
	}
	v, err = versions.ParseIntervals(rv)
	if err != nil {
		err = errors.Warning("websocket: parse X-Fns-Request-Version failed").WithCause(err)
		return
	}
	has = true
	return
}

func Succeed(payload any) (resp *Response) {
	resp = &Response{
		Succeed: true,
		Payload: payload,
	}
	return
}

func Failed(err error) (resp *Response) {
	resp = &Response{
		Succeed: false,
		Payload: errors.Wrap(err),
	}
	return
}

type Response struct {
	Succeed bool `json:"succeed"`
	Payload any  `json:"result"`
}

func (resp *Response) Encode() (p []byte) {
	var encodeErr error
	p, encodeErr = json.Marshal(resp)
	if encodeErr == nil {
		return
	}
	failed := &Response{
		Succeed: false,
		Payload: errors.Warning("websocket: encode response failed").WithCause(encodeErr),
	}
	p, _ = json.Marshal(failed)
	return
}

var (
	ErrRequestMessageIsTooLarge = fmt.Errorf("message is too large")
)

func readMessage(reader io.Reader, maxLimiter int64) (p []byte, err error) {
	buf := acquireBuf()
	for {
		n, readErr := reader.Read(buf)
		if n > 0 {
			p = append(p, buf[0:n]...)
		}
		if readErr != nil {
			if readErr == io.EOF {
				break
			}
			err = errors.Warning("websockets: read message failed").WithCause(readErr)
			break
		}
		if int64(len(p)) > maxLimiter {
			err = ErrRequestMessageIsTooLarge
			break
		}
	}
	releaseBuf(buf)
	if err != nil {
		return
	}
	return
}
