package generators

import (
	"context"
	"github.com/aacfactory/gcg"
)

// UseWriter
// @sql:use {endpointName}
type UseWriter struct {
}

func (writer *UseWriter) Annotation() (annotation string) {
	return "sql:use"
}

func (writer *UseWriter) HandleBefore(ctx context.Context, params []string, hasFnParam bool, hasFnResult bool) (code gcg.Code, err error) {
	return
}

func (writer *UseWriter) HandleAfter(ctx context.Context, params []string, hasFnParam bool, hasFnResult bool) (code gcg.Code, err error) {
	return
}

func (writer *UseWriter) ProxyBefore(ctx context.Context, params []string, hasFnParam bool, hasFnResult bool) (code gcg.Code, err error) {
	return
}

func (writer *UseWriter) ProxyAfter(ctx context.Context, params []string, hasFnParam bool, hasFnResult bool) (code gcg.Code, err error) {
	return
}
