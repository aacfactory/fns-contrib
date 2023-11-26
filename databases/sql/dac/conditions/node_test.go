package conditions_test

import (
	"context"
	"fmt"
	"reflect"
)

func RenderTODO(dict map[string]string) *RenderContext {
	return &RenderContext{
		Context:       context.TODO(),
		queryAcquires: 0,
		dict:          dict,
	}
}

type RenderContext struct {
	context.Context
	queryAcquires int
	dict          map[string]string
}

func (r *RenderContext) AcquireQueryPlaceholder() (v []byte) {
	r.queryAcquires++
	v = []byte(fmt.Sprintf("$%d", r.queryAcquires))
	return
}

func (r *RenderContext) Localization(key any) (content []byte, has bool) {
	if key == nil {
		return
	}
	name := ""
	switch k := key.(type) {
	case string:
		name = k
		break
	default:
		rt := reflect.TypeOf(key)
		if rt.Kind() != reflect.Struct {
			return
		}
		name = fmt.Sprintf("%s.%s", rt.PkgPath(), rt.Name())
		break
	}
	v, ok := r.dict[name]
	if ok {
		content = []byte(v)
		has = true
	}
	return
}
