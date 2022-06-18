package mysql

import (
	"context"
	"github.com/aacfactory/errors"
	"reflect"
)

type LoadMakeupHook interface {
	Makeup(ctx context.Context) (err errors.CodeError)
}

var loadMakeupHookType = reflect.TypeOf((*LoadMakeupHook)(nil)).Elem()

func executeLoadMakeupHook(ctx context.Context, row reflect.Value) (err errors.CodeError) {
	if !row.Type().Implements(loadMakeupHookType) {
		return
	}
	hookFn := row.MethodByName("Makeup")
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
