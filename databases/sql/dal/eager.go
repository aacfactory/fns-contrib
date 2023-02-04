package dal

import (
	"context"
	"github.com/aacfactory/errors"
)

func newEagerLoader(model *ModelStructure) (loader *EagerLoader, err errors.CodeError) {
	pks, hasPks := model.Pk()
	if !hasPks || len(pks) > 1 {
		err = errors.Warning("eager load mode only support one pk")
		return
	}
	loader = &EagerLoader{
		model: model,
		pk:    pks[0],
		keys:  make([]interface{}, 0, 1),
	}
	return
}

type EagerLoader struct {
	model *ModelStructure
	pk    *Field
	keys  []interface{}
}

func (el *EagerLoader) AppendKey(key interface{}) {
	for _, k := range el.keys {
		if k == key {
			return
		}
	}
	el.keys = append(el.keys, key)
}

func (el *EagerLoader) Load(ctx context.Context) (has bool, values map[interface{}]interface{}, err errors.CodeError) {
	//result, queryErr := query0(ctx, nil, nil, nil)
	// 把 query0 改成老版的，exported 的 还是泛型
	return
}
