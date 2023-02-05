package dal

import (
	"context"
	"github.com/aacfactory/errors"
	"reflect"
)

type keyable interface {
	~int | ~int8 | ~int16 | ~int32 | ~int64 |
		~uint | ~uint8 | ~uint16 | ~uint32 | ~uint64 | ~uintptr |
		~float32 | ~float64 | ~string
}

func QueryTree[T TreeModel, N keyable](ctx context.Context, conditions *Conditions, orders *Orders, rng *Range, rootNodeValue N) (result T, err errors.CodeError) {
	results, queryErr := queryTrees[T, N](ctx, conditions, orders, rng, rootNodeValue)
	if queryErr != nil {
		err = errors.ServiceError("dal: query tree failed").WithCause(queryErr)
		return
	}
	if results == nil || len(results) == 0 {
		return
	}
	result = results[rootNodeValue]
	return
}

func QueryTrees[T TreeModel, N keyable](ctx context.Context, conditions *Conditions, orders *Orders, rng *Range, rootNodeValues ...N) (results map[N]T, err errors.CodeError) {
	results, err = queryTrees[T, N](ctx, conditions, orders, rng, rootNodeValues...)
	if err != nil {
		err = errors.ServiceError("dal: query trees failed").WithCause(err)
		return
	}
	return
}

func queryTrees[T TreeModel, N keyable](ctx context.Context, conditions *Conditions, orders *Orders, rng *Range, rootNodeValues ...N) (results map[N]T, err errors.CodeError) {
	if rootNodeValues == nil || len(rootNodeValues) == 0 {
		err = errors.Warning("root node values are required")
		return
	}
	ctx = NotEagerLoad(ctx)
	// todo query
	list, queryErr := QueryWithRange[T](ctx, conditions, orders, rng)
	if queryErr != nil {
		err = queryErr
		return
	}
	results, err = mapListToTrees[T, N](list, rootNodeValues)
	return
}

func mapListToTrees[T TreeModel, N keyable](list []T, rootNodeValues []N) (nodes map[N]T, err errors.CodeError) {
	nodes = make(map[N]T)

	for _, rootNodeValue := range rootNodeValues {
		node, mapErr := mapListToTree[T, N](list, rootNodeValue)
		if mapErr != nil {
			err = mapErr
			return
		}
		if node == nil {
			continue
		}
		nodes[rootNodeValue] = node
	}
	return
}

func mapListToTree[T TreeModel, N keyable](list []T, rootNodeValue N) (node T, err errors.CodeError) {
	if list == nil || len(list) == 0 {
		return
	}
	currentFieldName, parentFieldName, childrenFieldName := list[0].NodeReferenceFields()
	for _, model := range list {
		rv := reflect.ValueOf(model).Elem()
		currentFieldValue, currentFieldValueTypeOk := rv.FieldByName(currentFieldName).Interface().(N)
		if !currentFieldValueTypeOk {
			err = errors.Warning("tree reference field value type is not matched").WithMeta("field", currentFieldName)
			return
		}
		if rootNodeValue != currentFieldValue {
			continue
		}
		// parent

	}
	return
}
