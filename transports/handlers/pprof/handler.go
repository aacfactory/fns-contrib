package pprof

import (
	"bytes"
	"github.com/aacfactory/errors"
	"github.com/aacfactory/fns/context"
	"github.com/aacfactory/fns/transports"
	"github.com/aacfactory/fns/transports/standard"
	"github.com/aacfactory/logs"
	"net/http/pprof"
	rtp "runtime/pprof"
)

var (
	pathPrefix  = []byte("/debug/pprof/")
	textContent = []byte("text/html")
)

var (
	cmdline = standard.ConvertHttpHandlerFunc(pprof.Cmdline)
	profile = standard.ConvertHttpHandlerFunc(pprof.Profile)
	symbol  = standard.ConvertHttpHandlerFunc(pprof.Symbol)
	trace   = standard.ConvertHttpHandlerFunc(pprof.Trace)
	index   = standard.ConvertHttpHandlerFunc(pprof.Index)
)

type Config struct {
	Enable bool `json:"enable"`
}

func New() transports.MuxHandler {
	return &handler{}
}

type handler struct {
	log    logs.Logger
	enable bool
}

func (h *handler) Name() string {
	return "pprof"
}

func (h *handler) Construct(options transports.MuxHandlerOptions) (err error) {
	h.log = options.Log
	config := Config{}
	configErr := options.Config.As(&config)
	if configErr != nil {
		err = errors.Warning("fns: construct pprof handler failed").WithCause(configErr)
		return
	}
	h.enable = config.Enable
	return
}

func (h *handler) Match(_ context.Context, method []byte, path []byte, _ transports.Header) bool {
	return h.enable && bytes.Equal(method, transports.MethodGet) && bytes.HasPrefix(path, pathPrefix)
}

func (h *handler) Handle(w transports.ResponseWriter, r transports.Request) {
	w.Header().Set(transports.ContentTypeHeaderName, textContent)
	switch {
	case bytes.HasPrefix(r.Path(), []byte("/debug/pprof/cmdline")):
		cmdline.Handle(w, r)
	case bytes.HasPrefix(r.Path(), []byte("/debug/pprof/profile")):
		profile.Handle(w, r)
	case bytes.HasPrefix(r.Path(), []byte("/debug/pprof/symbol")):
		symbol.Handle(w, r)
	case bytes.HasPrefix(r.Path(), []byte("/debug/pprof/trace")):
		trace.Handle(w, r)
	default:
		for _, v := range rtp.Profiles() {
			ppName := v.Name()
			if bytes.HasPrefix(r.Path(), []byte("/debug/pprof/"+ppName)) {
				namedHandler := standard.ConvertHttpHandlerFunc(pprof.Handler(ppName).ServeHTTP)
				namedHandler.Handle(w, r)
				return
			}
		}
		index.Handle(w, r)
	}
	return
}
