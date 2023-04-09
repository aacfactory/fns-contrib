package websockets

import (
	"fmt"
	"github.com/aacfactory/errors"
	"github.com/aacfactory/fns/service"
	"github.com/aacfactory/fns/service/transports"
	"github.com/aacfactory/json"
	"github.com/valyala/bytebufferpool"
	"io"
)

func NewRequest(service string, fn string, payload interface{}) (r *Request, err error) {
	if service == "" || fn == "" {
		err = errors.Warning("websockets: new request failed").WithCause(errors.Warning("service and fn is required"))
		return
	}
	var p []byte
	if payload == nil {
		p = []byte{'{', '}'}
	} else {
		p, err = json.Marshal(payload)
		if err != nil {
			err = errors.Warning("websockets: new request failed").WithCause(errors.Warning("encode payload failed").WithCause(err))
			return
		}
	}
	r = &Request{
		Service: service,
		Fn:      fn,
		Header:  make(transports.Header),
		Payload: p,
	}
	return
}

type Request struct {
	Service string            `json:"service"`
	Fn      string            `json:"fn"`
	Header  transports.Header `json:"header"`
	Payload json.RawMessage   `json:"payload"`
}

func (request *Request) Validate() (err error) {
	if request.Service == "" || request.Fn == "" {
		err = errors.Warning("websocket: invalid request")
		return
	}
	return
}

func (request *Request) Versions() (v service.RequestVersions, err error) {
	rvs, hasVersion, parseVersionErr := service.ParseRequestVersionFromHeader(request.Header)
	if parseVersionErr != nil {
		err = errors.Warning("fns: parse X-Fns-Request-Version failed").WithCause(parseVersionErr)
		return
	}
	if !hasVersion {
		rvs = service.AllowAllRequestVersions()
	}
	v = rvs
	return
}

func Succeed(payload interface{}) (resp *Response) {
	resp = &Response{
		Succeed: true,
		Payload: payload,
	}
	return
}

func Failed(err error) (resp *Response) {
	resp = &Response{
		Succeed: false,
		Payload: errors.Map(err),
	}
	return
}

type Response struct {
	Succeed bool        `json:"succeed"`
	Payload interface{} `json:"result"`
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
	b := bytebufferpool.Get()
	buf := acquireBuf()
	for {
		n, readErr := reader.Read(buf)
		if n > 0 {
			_, _ = b.Write(buf[0:n])
		}
		if readErr != nil {
			if readErr == io.EOF {
				break
			}
			err = errors.Warning("websockets: read message failed").WithCause(readErr)
			break
		}
		if int64(b.Len()) > maxLimiter {
			err = ErrRequestMessageIsTooLarge
			break
		}
	}
	if err != nil {
		releaseBuf(buf[:])
		bytebufferpool.Put(b)
		return
	}
	p = b.Bytes()
	releaseBuf(buf[:])
	bytebufferpool.Put(b)
	return
}
