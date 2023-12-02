package generators

import (
	"context"
	"fmt"
	"github.com/aacfactory/errors"
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
	paramsLen := len(params)
	if paramsLen != 1 {
		err = errors.Warning("sql: generate use code failed").WithCause(fmt.Errorf("invalid annotation params"))
		return
	}
	name := params[0]

	stmt := gcg.Statements()
	stmt.Tab().Token(fmt.Sprintf("sql.Use(ctx, bytex.FromString(\"%s\"))", name),
		gcg.NewPackage("github.com/aacfactory/fns/commons/bytex"),
		gcg.NewPackage("github.com/aacfactory/fns-contrib/databases/sql"),
	).Line()

	code = stmt
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
