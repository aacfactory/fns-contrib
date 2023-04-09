package profiling

import (
	"bytes"
	"github.com/aacfactory/errors"
	"github.com/aacfactory/fns/commons/bytex"
	"github.com/aacfactory/fns/service/transports"
	"net/http/pprof"
)

const (
	httpPprofPath        = "/debug/pprof"
	httpPprofCmdlinePath = "/debug/pprof/cmdline"
	httpPprofProfilePath = "/debug/pprof/profile"
	httpPprofSymbolPath  = "/debug/pprof/symbol"
	httpPprofTracePath   = "/debug/pprof/trace"
)

type Config struct {
	Enable bool `json:"enable"`
}

func Handler() (h transports.Handler) {
	h = &pprofHandler{}
	return
}

type pprofHandler struct {
	enable bool
}

func (h *pprofHandler) Name() (name string) {
	name = "pprof"
	return
}

func (h *pprofHandler) Build(options *transports.Options) (err error) {
	config := &Config{}
	configErr := options.Config.As(config)
	if configErr != nil {
		err = errors.Warning("fns: build pprof handler failed").WithCause(configErr)
		return
	}
	h.enable = config.Enable
	return
}

func (h *pprofHandler) Accept(request *transports.Request) (ok bool) {
	ok = bytes.Index(request.Path(), bytex.FromString(httpPprofPath)) == 0
	return
}

func (h *pprofHandler) Handle(writer transports.ResponseWriter, request *transports.Request) {
	if !h.enable {
		writer.Failed(errors.Warning("fns: disabled").WithMeta("handler", h.Name()))
		return
	}
	r, rErr := transports.ConvertRequestToHttpRequest(request)
	if rErr != nil {
		writer.Failed(errors.Warning("fns: handle failed").WithCause(rErr).WithMeta("handler", h.Name()))
		return
	}
	w := transports.ConvertResponseWriterToHttpResponseWriter(writer)
	switch bytex.ToString(request.Path()) {
	case httpPprofPath:
		pprof.Index(w, r)
		break
	case httpPprofCmdlinePath:
		pprof.Cmdline(w, r)
		break
	case httpPprofProfilePath:
		pprof.Profile(w, r)
		break
	case httpPprofSymbolPath:
		pprof.Symbol(w, r)
		break
	case httpPprofTracePath:
		pprof.Trace(w, r)
		break
	default:
		break
	}
	return
}

func (h *pprofHandler) Close() {
}
