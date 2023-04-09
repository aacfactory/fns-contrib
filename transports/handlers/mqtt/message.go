package mqtt

import (
	"github.com/aacfactory/errors"
	"github.com/aacfactory/json"
	"net/http"
)

type Request struct {
	Service string          `json:"service"`
	Fn      string          `json:"fn"`
	Header  http.Header     `json:"header"`
	Payload json.RawMessage `json:"payload"`
}

func (request *Request) Validate() (err error) {
	if request.Service == "" || request.Fn == "" || request.Header == nil || len(request.Header) == 0 {
		err = errors.Warning("mqtt: invalid request")
		return
	}
	if request.DeviceId() == "" {
		err = errors.Warning("mqtt: invalid request")
		return
	}
	return
}

func (request *Request) DeviceId() (id string) {
	request.Header.Get("X-Fns-Device-Id")
	return
}

func (request *Request) DeviceIp() (id string) {
	request.Header.Get("X-Fns-Device-Ip")
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
		Payload: errors.Warning("mqtt: encode response failed").WithCause(encodeErr),
	}
	p, _ = json.Marshal(failed)
	return
}
