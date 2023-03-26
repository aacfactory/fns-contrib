package dal

import (
	"context"
	"github.com/aacfactory/errors"
	"reflect"
	"sort"
)

type keyable interface {
	~int | ~int8 | ~int16 | ~int32 | ~int64 |
		~uint | ~uint8 | ~uint16 | ~uint32 | ~uint64 | ~uintptr |
		~float32 | ~float64 | ~string
}

func QueryTree[T Model, N keyable](ctx context.Context, conditions *Conditions, orders *Orders, rng *Range, rootNodeValue N) (result T, err errors.CodeError) {
	results, queryErr := queryTrees[T, N](ctx, conditions, orders, rng, rootNodeValue)
	if queryErr != nil {
		err = errors.ServiceError("dal: query tree failed").WithCause(queryErr)
		return
	}
	if results == nil || len(results) == 0 {
		return
	}
	result = results[0]
	return
}

func QueryTrees[T Model, N keyable](ctx context.Context, conditions *Conditions, orders *Orders, rng *Range, rootNodeValues ...N) (results []T, err errors.CodeError) {
	results, err = queryTrees[T, N](ctx, conditions, orders, rng, rootNodeValues...)
	if err != nil {
		err = errors.ServiceError("dal: query trees failed").WithCause(err)
		return
	}
	return
}

func queryTrees[T Model, N keyable](ctx context.Context, conditions *Conditions, orders *Orders, rng *Range, rootNodeValues ...N) (results []T, err errors.CodeError) {
	if rootNodeValues == nil || len(rootNodeValues) == 0 {
		err = errors.Warning("root node values are required")
		return
	}
	ctx = NotEagerLoad(ctx)
	list, queryErr := QueryWithRange[T](ctx, conditions, orders, rng)
	if queryErr != nil {
		err = queryErr
		return
	}
	results, err = MapListToTrees[T, N](list, rootNodeValues)
	return
}

func MapListToTrees[T Model, N keyable](list []T, rootNodeValues []N) (nodes []T, err errors.CodeError) {
	nodes = make([]T, 0, 1)
	for _, rootNodeValue := range rootNodeValues {
		node, mapErr := MapListToTree[T, N](list, rootNodeValue)
		if mapErr != nil {
			err = mapErr
			return
		}
		if reflect.ValueOf(node).IsNil() {
			continue
		}
		nodes = append(nodes, node)
	}
	return
}

func MapListToTree[T Model, N keyable](list []T, rootNodeValue N) (node T, err errors.CodeError) {
	if list == nil || len(list) == 0 {
		return
	}
	structure, getStructureErr := getModelStructure(list[0])
	if getStructureErr != nil {
		err = errors.Warning("get model structure failed").WithCause(getStructureErr)
		return
	}
	var field *Field
	for _, f := range structure.fields {
		if f.IsTreeType() {
			field = f
			break
		}
	}
	if field == nil {
		err = errors.Warning("tree field was not found")
		return
	}
	nodeField, hasNodeField := structure.FindFieldByColumn(field.tree.nodeColumnName)
	if !hasNodeField {
		err = errors.Warning("node field was not found").WithMeta("node_column", field.tree.nodeColumnName)
		return
	}
	parentField, hasParentField := structure.FindFieldByColumn(field.tree.parentColumnName)
	if !hasParentField {
		err = errors.Warning("parent field was not found").WithMeta("node_column", field.tree.parentColumnName)
		return
	}

	for _, model := range list {
		rv := reflect.ValueOf(model).Elem()
		nodeFieldValue, nodeFieldValueTypeOk := rv.FieldByName(nodeField.name).Interface().(N)
		if !nodeFieldValueTypeOk {
			err = errors.Warning("tree node field value type is not matched").WithMeta("field", nodeField.name)
			return
		}
		if rootNodeValue != nodeFieldValue {
			continue
		}
		// children
		childrenField := rv.FieldByName(field.name)
		children := make([]T, 0, 1)
		for _, child := range list {
			childValue := reflect.ValueOf(child)
			childFieldValue, childFieldValueTypeOk := childValue.Elem().FieldByName(nodeField.name).Interface().(N)
			if !childFieldValueTypeOk {
				err = errors.Warning("tree node field value type is not matched").WithMeta("field", nodeField.name)
				return
			}
			parentFieldValue, parentFieldValueTypeOk := childValue.Elem().FieldByName(parentField.name).Interface().(N)
			if !parentFieldValueTypeOk {
				err = errors.Warning("tree node field value type is not matched").WithMeta("field", parentField.name)
				return
			}
			if parentFieldValue != nodeFieldValue {
				continue
			}
			childNode, mapErr := MapListToTree[T, N](list, childFieldValue)
			if mapErr != nil {
				err = mapErr
				return
			}
			children = append(children, childNode)
		}
		childrenField.Set(reflect.ValueOf(children))
		if childrenField.Type().ConvertibleTo(sortType) {
			sortable := childrenField.Convert(sortType).Interface().(sort.Interface)
			sort.Sort(sortable)
		}
		node = model
		break
	}
	return
}
