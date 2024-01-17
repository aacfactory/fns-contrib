package documents

import (
	"bytes"
	"embed"
	"fmt"
	"github.com/aacfactory/errors"
	"github.com/aacfactory/fns-contrib/transports/handlers/documents/oas"
	"github.com/aacfactory/fns-contrib/transports/handlers/documents/spec"
	"github.com/aacfactory/fns/commons/bytex"
	"github.com/aacfactory/fns/commons/versions"
	"github.com/aacfactory/fns/context"
	"github.com/aacfactory/fns/runtime"
	"github.com/aacfactory/fns/services"
	"github.com/aacfactory/fns/transports"
	"github.com/aacfactory/logs"
	"golang.org/x/sync/singleflight"
	"sort"
	"strings"
)

var (
	_path                 = []byte("/documents")
	_viewPathPrefix       = []byte("/documents/view/")
	_oasViewPathPrefix    = []byte("/documents/openapi/")
	openapiQueryParam     = []byte("openapi")
	htmlContentType       = []byte("text/html")
	jsContentType         = []byte("text/javascript")
	cssContentType        = []byte("text/css")
	viewDirPath           = []byte("view/")
	oasDirPath            = []byte("openapi/")
	viewIndexHtmlFilename = []byte("index.html")
	jsSuffix              = []byte(".js")
	cssSuffix             = []byte(".css")
	maxAge                = []byte("max-age=64000")
)

const (
	groupKey = "documents"
)

//go:embed view
var view embed.FS

//go:embed openapi
var openapi embed.FS

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
	openAPI OpenAPI
	group   singleflight.Group
}

func (handler *Handler) Name() string {
	return "documents"
}

func (handler *Handler) Construct(options transports.MuxHandlerOptions) (err error) {
	handler.log = options.Log
	config := Config{}
	configErr := options.Config.As(&config)
	if configErr != nil {
		err = errors.Warning("fns: construct documents handler failed").WithCause(configErr)
		return
	}
	handler.enable = config.Enable
	if handler.enable {
		handler.openAPI = config.OpenAPI
		if handler.openAPI.Title == "" {
			handler.openAPI.Title = "FNS"
		}
	}
	return
}

func (handler *Handler) Match(_ context.Context, method []byte, path []byte, _ transports.Header) (ok bool) {
	if !handler.enable {
		return
	}
	if bytes.Equal(method, transports.MethodGet) {
		if ok = bytes.Equal(path, _path); ok {
			return
		}
		if ok = bytes.Index(path, _viewPathPrefix) == 0; ok {
			return
		}
		if ok = bytes.Index(path, _oasViewPathPrefix) == 0; ok {
			return
		}
		return
	}
	return
}

func (handler *Handler) Handle(w transports.ResponseWriter, r transports.Request) {
	path := r.Path()
	if bytes.Index(path, _viewPathPrefix) == 0 {
		static, found := bytes.CutPrefix(path, _viewPathPrefix)
		if !found || len(static) == 0 {
			static = viewIndexHtmlFilename
		}
		contentType := htmlContentType
		if _, foundJs := bytes.CutSuffix(static, jsSuffix); foundJs {
			contentType = jsContentType
		}
		if _, foundCss := bytes.CutSuffix(static, cssSuffix); foundCss {
			contentType = cssContentType
		}
		static = append(viewDirPath, static...)
		p, readErr := view.ReadFile(bytex.ToString(static))
		if readErr != nil {
			s := fmt.Sprintf("%+v", errors.Warning(fmt.Sprintf("documents: read %s failed", bytex.ToString(static))).WithMeta("file", bytex.ToString(static)).WithCause(readErr))
			s = strings.ReplaceAll(s, "\n", "<br>")
			p = bytex.FromString(s)
			contentType = htmlContentType
		} else {
			w.Header().Set(transports.CacheControlHeaderName, maxAge)
		}
		w.Header().Set(transports.ContentTypeHeaderName, contentType)
		_, _ = w.Write(p)
		return
	}
	if bytes.Index(path, _oasViewPathPrefix) == 0 {
		static, found := bytes.CutPrefix(path, _oasViewPathPrefix)
		if !found || len(static) == 0 {
			static = viewIndexHtmlFilename
		}
		contentType := htmlContentType
		if _, foundJs := bytes.CutSuffix(static, jsSuffix); foundJs {
			contentType = jsContentType
		}
		if _, foundCss := bytes.CutSuffix(static, cssSuffix); foundCss {
			contentType = cssContentType
		}
		static = append(oasDirPath, static...)
		p, readErr := openapi.ReadFile(bytex.ToString(static))
		if readErr != nil {
			s := fmt.Sprintf("%+v", errors.Warning(fmt.Sprintf("documents: read %s failed", bytex.ToString(static))).WithMeta("file", bytex.ToString(static)).WithCause(readErr))
			s = strings.ReplaceAll(s, "\n", "<br>")
			p = bytex.FromString(s)
			contentType = htmlContentType
		} else {
			w.Header().Set(transports.CacheControlHeaderName, maxAge)
		}
		w.Header().Set(transports.ContentTypeHeaderName, contentType)
		_, _ = w.Write(p)
		return
	}
	openapiParam := r.Params().Get(openapiQueryParam)
	data := handler.documents(r)
	if len(openapiParam) == 0 {
		w.Succeed(data)
	} else {
		if len(data) == 0 {
			w.Succeed(services.Empty{})
			return
		}
		var target spec.Document
		if string(openapiParam) == "latest" {
			target = data.Latest()
		} else {
			version, versionErr := versions.Parse(openapiParam)
			if versionErr == nil {
				target = data.MatchMajorAndMiner(version)
			} else {
				target = spec.Document{
					Version:   versions.Version{},
					Endpoints: nil,
				}
			}
		}
		api := oas.Openapi(handler.openAPI.Title, handler.openAPI.Description, handler.openAPI.Term, handler.openAPI.Version, target)
		w.Succeed(api)
	}
}

func (handler *Handler) documents(r transports.Request) (v spec.Documents) {
	eps := runtime.Endpoints(r)
	vv, _, _ := handler.group.Do(groupKey, func() (v interface{}, err error) {
		dd := make(spec.Documents, 0, 1)
		infos := eps.Info()
		for _, info := range infos {
			if ep := info.Document; ep.Defined() {
				dd = dd.Add(ep)
			}
		}
		sort.Sort(dd)
		v = dd
		return
	})
	handler.group.Forget(groupKey)
	v = vv.(spec.Documents)
	return
}
