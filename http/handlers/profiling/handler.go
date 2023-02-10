package profiling

import (
	"github.com/aacfactory/errors"
	"github.com/aacfactory/fns/server"
	"net/http"
	"net/http/pprof"
	"strings"
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

func Handler() (h server.Handler) {
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

func (h *pprofHandler) Build(options *server.HandlerOptions) (err error) {
	config := &Config{}
	configErr := options.Config.As(config)
	if configErr != nil {
		err = errors.Warning("fns: build pprof handler failed").WithCause(configErr)
		return
	}
	h.enable = config.Enable
	return
}

func (h *pprofHandler) Accept(request *http.Request) (ok bool) {
	ok = strings.Index(request.URL.Path, httpPprofPath) == 0
	return
}

func (h *pprofHandler) ServeHTTP(writer http.ResponseWriter, request *http.Request) {
	if !h.enable {
		writer.WriteHeader(http.StatusServiceUnavailable)
		return
	}
	switch request.URL.Path {
	case httpPprofPath:
		pprof.Index(writer, request)
		break
	case httpPprofCmdlinePath:
		pprof.Cmdline(writer, request)
		break
	case httpPprofProfilePath:
		pprof.Profile(writer, request)
		break
	case httpPprofSymbolPath:
		pprof.Symbol(writer, request)
		break
	case httpPprofTracePath:
		pprof.Trace(writer, request)
		break
	default:
		break
	}
	return
}

func (h *pprofHandler) Close() {
}
