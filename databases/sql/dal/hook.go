package dal

import (
	"github.com/aacfactory/errors"
	"github.com/aacfactory/fns/context"
	"reflect"
)

type ModelLoadHook interface {
	AfterLoaded(ctx context.Context) (err error)
}

var modelLoadHookType = reflect.TypeOf((*ModelLoadHook)(nil)).Elem()

func executeModelLoadHook(ctx context.Context, resultPtrValue reflect.Value) (err error) {
	if !resultPtrValue.Type().Implements(modelLoadHookType) {
		return
	}
	hookFn := resultPtrValue.MethodByName("AfterLoaded")
	results := hookFn.Call([]reflect.Value{reflect.ValueOf(ctx)})
	if results == nil || len(results) == 0 {
		return
	}
	errValue := results[0]
	if errValue.IsNil() {
		return
	}
	err = errValue.Interface().(errors.CodeError)
	return
}
