package sql

import (
	"github.com/aacfactory/errors"
	"github.com/aacfactory/fns/context"
	"github.com/aacfactory/fns/runtime"
	"github.com/aacfactory/fns/services"
)

var (
	dialectFnName           = []byte("dialect")
	dialectContextKeyPrefix = []byte("@fns:sql:dialect:")
)

func ForceDialect(ctx context.Context, dialect string) context.Context {
	ep := endpointName
	if epn := used(ctx); len(epn) > 0 {
		ep = epn
	}
	key := append(dialectContextKeyPrefix, ep...)
	stored, has := context.LocalValue[string](ctx, key)
	if has && stored == dialect {
		return ctx
	}
	ctx.SetLocalValue(key, dialect)
	return ctx
}

func Dialect(ctx context.Context) (dialect string, err error) {
	ep := endpointName
	if epn := used(ctx); len(epn) > 0 {
		ep = epn
	}
	key := append(dialectContextKeyPrefix, ep...)
	has := false
	dialect, has = context.LocalValue[string](ctx, key)
	if has {
		return
	}
	eps := runtime.Endpoints(ctx)
	response, handleErr := eps.Request(ctx, ep, dialectFnName, nil)
	if handleErr != nil {
		err = handleErr
		return
	}
	dialect, err = services.ValueOfResponse[string](response)
	if err != nil {
		err = errors.Warning("sql: dialect failed").WithCause(err)
		return
	}
	ctx.SetLocalValue(key, dialect)
	return
}

type dialectFn struct {
	dialect string
}

func (fn *dialectFn) Name() string {
	return string(dialectFnName)
}

func (fn *dialectFn) Internal() bool {
	return true
}

func (fn *dialectFn) Readonly() bool {
	return false
}

func (fn *dialectFn) Handle(_ services.Request) (v interface{}, err error) {
	v = fn.dialect
	return
}
