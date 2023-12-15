package documents

import (
	"bytes"
	"github.com/aacfactory/errors"
	"github.com/aacfactory/fns-contrib/transports/handlers/documents/oas"
	"github.com/aacfactory/fns/commons/versions"
	"github.com/aacfactory/fns/context"
	"github.com/aacfactory/fns/runtime"
	"github.com/aacfactory/fns/services"
	"github.com/aacfactory/fns/services/documents"
	"github.com/aacfactory/fns/transports"
	"github.com/aacfactory/logs"
	"golang.org/x/sync/singleflight"
)

var (
	_path             = []byte("/documents")
	openapiQueryParam = []byte("openapi")
)

const (
	groupKey = "documents"
)

func New() transports.MuxHandler {
	return &Handler{}
}

// Handler
// method is get
// url is /documents
// when /documents?openapi={latest|version_text}, then return openapi
type Handler struct {
	log     logs.Logger
	enable  bool
	OpenAPI OpenAPI
	group   singleflight.Group
}

func (handler *Handler) Name() string {
	return "documents"
}

func (handler *Handler) Construct(options transports.MuxHandlerOptions) (err error) {
	handler.log = options.Log
	config := Config{}
	configErr := options.Config.As(&Config{})
	if configErr != nil {
		err = errors.Warning("fns: construct documents handler failed").WithCause(configErr)
		return
	}
	handler.enable = config.Enable
	handler.OpenAPI = config.OpenAPI
	if handler.OpenAPI.Title == "" {
		handler.OpenAPI.Title = "FNS"
	}
	return
}

func (handler *Handler) Match(_ context.Context, method []byte, path []byte, _ transports.Header) (ok bool) {
	if !handler.enable {
		return
	}
	ok = bytes.Equal(method, transports.MethodGet) && bytes.Equal(_path, path)
	return
}

func (handler *Handler) Handle(w transports.ResponseWriter, r transports.Request) {
	openapiParam := r.Params().Get(openapiQueryParam)
	data := handler.documents(r)
	if len(openapiParam) == 0 {
		w.Succeed(data)
	} else {
		if len(data) == 0 {
			w.Succeed(services.Empty{})
		}
		var target documents.Document
		has := false
		if string(openapiParam) == "latest" {
			target = data.Latest()
			has = true
		} else {
			version, versionErr := versions.Parse(openapiParam)
			if versionErr == nil {
				target, has = data.Version(version)
			}
		}
		if !has {
			target = data.Latest()
		}
		api := oas.Openapi(handler.OpenAPI.Title, handler.OpenAPI.Description, handler.OpenAPI.Term, handler.OpenAPI.Version, target)
		w.Succeed(api)
	}
}

func (handler *Handler) documents(r transports.Request) (v documents.Documents) {
	eps := runtime.Endpoints(r)
	vv, _, _ := handler.group.Do(groupKey, func() (v interface{}, err error) {
		dd := make(documents.Documents, 0, 1)
		infos := eps.Info()
		for _, info := range infos {
			if ep := info.Document; ep.Defined() {
				dd = dd.Add(ep)
			}
		}
		v = dd
		return
	})
	v = vv.(documents.Documents)
	return
}
