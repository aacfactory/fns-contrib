package websockets

import (
	"github.com/aacfactory/errors"
	"github.com/aacfactory/fns/service/transports"
	"github.com/aacfactory/json"
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
