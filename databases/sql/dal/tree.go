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

func QueryTree[T Model, N keyable](ctx context.Context, conditions *Conditions, orders *Orders, rng *Range, nodeValue N) (result T, err errors.CodeError) {
	results, queryErr := queryTrees[T, N](ctx, conditions, orders, rng, nodeValue)
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

func QueryTrees[T Model, N keyable](ctx context.Context, conditions *Conditions, orders *Orders, rng *Range, nodeValues ...N) (results []T, err errors.CodeError) {
	results, err = queryTrees[T, N](ctx, conditions, orders, rng, nodeValues...)
	if err != nil {
		err = errors.ServiceError("dal: query trees failed").WithCause(err)
		return
	}
	return
}

func QueryRootTree[T Model, N keyable](ctx context.Context, conditions *Conditions, orders *Orders, rng *Range) (result T, err errors.CodeError) {
	results, queryErr := QueryRootTrees[T, N](ctx, conditions, orders, rng)
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

func QueryRootTrees[T Model, N keyable](ctx context.Context, conditions *Conditions, orders *Orders, rng *Range) (results []T, err errors.CodeError) {
	results, err = QueryTrees[T, N](ctx, conditions, orders, rng)
	return
}

func queryTrees[T Model, N keyable](ctx context.Context, conditions *Conditions, orders *Orders, rng *Range, rootNodeValues ...N) (results []T, err errors.CodeError) {
	ctx = NotEagerLoad(ctx)
	list, queryErr := QueryWithRange[T](ctx, conditions, orders, rng)
	if queryErr != nil {
		err = queryErr
		return
	}
	if list == nil || len(list) == 0 {
		return
	}
	results, err = MapListToTrees[T, N](list, rootNodeValues)
	return
}

func MapListToTrees[T Model, N keyable](list []T, rootNodeValues []N) (nodes []T, err errors.CodeError) {
	structure, field, fieldErr := getTreeModelKeyFieldName(list)
	if fieldErr != nil {
		err = fieldErr
		return
	}
	if rootNodeValues == nil || len(rootNodeValues) == 0 || reflect.ValueOf(rootNodeValues[0]).IsZero() {
		nodeField, hasNodeField := structure.FindFieldByColumn(field.tree.nodeColumnName)
		if !hasNodeField {
			err = errors.Warning("tree node need node field")
			return
		}
		parentField, hasParentField := structure.FindFieldByColumn(field.tree.parentColumnName)
		if !hasParentField {
			err = errors.Warning("tree node need parent field")
			return
		}
		rootNodeValues = make([]N, 0, 1)
		for _, item := range list {
			rv := reflect.Indirect(reflect.ValueOf(item))
			parent := rv.FieldByName(parentField.Name())
			if parent.IsZero() {
				node := rv.FieldByName(nodeField.Name())
				if node.IsZero() {
					continue
				}
				nodeValue, isN := node.Interface().(N)
				if !isN {
					continue
				}
				rootNodeValues = append(rootNodeValues, nodeValue)
			}
		}
	}
	nodes = make([]T, 0, 1)
	for _, rootNodeValue := range rootNodeValues {
		contains := false
		for _, prev := range nodes {
			if containsTreeModel(prev, rootNodeValue, structure, field) {
				contains = true
				break
			}
		}
		if contains {
			continue
		}
		node, mapErr := MapListToTree[T, N](list, rootNodeValue)
		if mapErr != nil {
			err = mapErr
			return
		}
		if reflect.ValueOf(node).IsNil() {
			continue
		}
		ejects := make([]int, 0, 1)
		for i, prev := range nodes {
			rv := reflect.Indirect(reflect.ValueOf(prev))
			nodeField, hasNodeField := structure.FindFieldByColumn(field.tree.nodeColumnName)
			if !hasNodeField {
				continue
			}
			prevKey, isN := rv.FieldByName(nodeField.Name()).Interface().(N)
			if !isN {
				continue
			}
			if containsTreeModel(node, prevKey, structure, field) {
				ejects = append(ejects, i)
			}
		}
		ejectsLen := len(ejects)
		if ejectsLen > 0 {
			temps := make([]T, 0, 1)
			for i, prev := range nodes {
				eject := sort.Search(ejectsLen, func(j int) bool {
					return ejects[j] == i
				})
				if eject == ejectsLen {
					temps = append(temps, prev)
				}
			}
			nodes = temps
		}
		nodes = append(nodes, node)
	}
	return
}

func containsTreeModel[T Model, N keyable](node T, key N, structure *ModelStructure, f *Field) (ok bool) {
	rv := reflect.Indirect(reflect.ValueOf(node))
	nodeField, hasNodeField := structure.FindFieldByColumn(f.tree.nodeColumnName)
	if !hasNodeField {
		return
	}
	field := rv.FieldByName(nodeField.Name())
	if field.IsZero() {
		return
	}
	fv, isN := field.Interface().(N)
	if !isN {
		return
	}
	if fv == key {
		ok = true
		return
	}
	childrenField := rv.FieldByName(f.Name())
	childrenType := reflect.TypeOf(make([]T, 0, 1))
	if !childrenField.CanConvert(childrenType) {
		return
	}
	childrenField = childrenField.Convert(childrenType)
	if childrenField.IsNil() {
		return
	}
	children, isModels := childrenField.Interface().([]T)
	if !isModels {
		return
	}
	if children == nil || len(children) == 0 {
		return
	}
	for _, child := range children {
		ok = containsTreeModel(child, key, structure, f)
		if ok {
			return
		}
	}
	return
}

func getTreeModelKeyFieldName[T Model](list []T) (structure *ModelStructure, field *Field, err errors.CodeError) {
	structure0, getStructureErr := getModelStructure(list[0])
	if getStructureErr != nil {
		err = errors.Warning("dal: get tree model key field failed").WithCause(getStructureErr)
		return
	}
	structure = structure0
	for _, f := range structure.fields {
		if f.IsTreeType() {
			field = f
			return
		}
	}
	if field == nil {
		err = errors.Warning("dal: get tree model key field failed").WithCause(errors.Warning("tree field was not found"))
		return
	}
	return
}

func MapListToTree[T Model, N keyable](list []T, rootNodeValue N) (node T, err errors.CodeError) {
	if list == nil || len(list) == 0 {
		return
	}
	structure, field, fieldErr := getTreeModelKeyFieldName(list)
	if fieldErr != nil {
		err = fieldErr
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
