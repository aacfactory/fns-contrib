package dal

import (
	"context"
	"github.com/aacfactory/errors"
	"reflect"
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
	conditions := NewConditions(IN(el.pk.Column(), el.keys))
	resultsValue := reflect.MakeSlice(reflect.SliceOf(el.model.Type()), 0, 1)
	results := resultsValue.Interface()
	queryErr := query0(ctx, conditions, nil, nil, &results)
	if queryErr != nil {
		err = errors.ServiceError("eager load failed").WithCause(queryErr)
		return
	}
	values = make(map[interface{}]interface{})
	resultsValueLen := resultsValue.Len()
	for i := 0; i < resultsValueLen; i++ {
		resultValue := resultsValue.Index(i)
		pk := resultValue.Elem().FieldByName(el.pk.Name()).Interface()
		values[pk] = resultValue.Interface()
	}
	return
}
