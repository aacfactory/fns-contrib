package dal

import (
	"context"
	"fmt"
	"github.com/aacfactory/errors"
	"github.com/aacfactory/fns-contrib/databases/sql"
	"golang.org/x/sync/singleflight"
	"reflect"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
)

// Model
// @title 数据表模型
// @description 数据表模型
type Model interface {
	TableName() (schema string, name string)
}

func newModel[T Model]() (v T) {
	m := new(T)
	v = *m
	return
}

var (
	sortType        = reflect.TypeOf((*sort.Interface)(nil)).Elem()
	modelType       = reflect.TypeOf((*Model)(nil)).Elem()
	modelStructures = new(sync.Map)
	gettingBarrier  = new(singleflight.Group)
)

func getModelStructReflectType(model Model) (rt reflect.Type) {
	rt = reflect.TypeOf(model)
	for {
		if rt.Kind() == reflect.Struct {
			return
		}
		rt = rt.Elem()
	}
}

func implementsModel(v interface{}) (ok bool) {
	ok = reflect.TypeOf(v).Implements(modelType)
	return
}

func isModelType(typ reflect.Type) (ok bool) {
	ok = typ.Implements(modelType)
	return
}

func newModelByType(rt reflect.Type) (model Model) {
	if rt.Kind() == reflect.Ptr {
		rt = rt.Elem()
	}
	model = reflect.New(rt).Interface().(Model)
	return
}

func getModelQueryGenerator(ctx context.Context, model Model) (structure *ModelStructure, generator QueryGenerator, err errors.CodeError) {
	dialectName, getDialectErr := sql.Dialect(ctx)
	if getDialectErr != nil {
		err = errors.Warning("dal: get dialect failed").WithCause(getDialectErr)
		return
	}
	var getStructureErr error
	structure, getStructureErr = getModelStructure(model)
	if getStructureErr != nil {
		err = errors.Warning("dal: get model structure failed").WithCause(getStructureErr)
		return
	}
	var generatorErr error
	generator, generatorErr = structure.DialectQueryGenerator(Dialect(dialectName))
	if generatorErr != nil {
		err = errors.Warning("dal: get model query generator failed").WithCause(generatorErr)
		return
	}
	return
}

func StructureOfModel(model Model) (structure *ModelStructure, err error) {
	structure, err = getModelStructure(model)
	return
}

func getModelStructure(model Model) (structure *ModelStructure, err error) {
	if model == nil {
		err = errors.Warning("get model structure failed cause model is nil")
		return
	}
	rt := getModelStructReflectType(model)
	key := fmt.Sprintf("%s.%s", rt.PkgPath(), rt.Name())
	schema, name := model.TableName()
	schema = strings.TrimSpace(schema)
	name = strings.TrimSpace(name)
	if name == "" {
		err = errors.Warning(fmt.Sprintf("get model structure failed cause %s model has no table name", key))
		return
	}
	stored, loaded := modelStructures.Load(key)
	if loaded {
		structure = stored.(*ModelStructure)
		return
	}
	result, executeErr, _ := gettingBarrier.Do(key, func() (v interface{}, doErr error) {
		structure = &ModelStructure{
			typ:             rt,
			schema:          schema,
			name:            name,
			fields:          make([]*Field, 0, 1),
			queryGenerators: new(sync.Map),
		}
		modelStructures.Store(key, structure)
		doErr = structure.scanReflectType(rt)
		if doErr != nil {
			return
		}
		structure.scanAbstractedFields(newModelStructureReferencePath(structure))
		v = structure
		return
	})
	gettingBarrier.Forget(key)
	if executeErr != nil {
		err = errors.Warning(fmt.Sprintf("get model structure of %s model failed", key)).WithCause(executeErr)
		return
	}
	structure = result.(*ModelStructure)
	return
}

type ModelStructure struct {
	typ             reflect.Type
	schema          string
	name            string
	fields          []*Field
	queryGenerators *sync.Map
}

func (structure *ModelStructure) DialectQueryGenerator(dialect Dialect) (queryGenerator QueryGenerator, err error) {
	stored, loaded := structure.queryGenerators.Load(dialect)
	if loaded {
		cached, has := stored.(QueryGenerator)
		if !has {
			err = fmt.Errorf("%s query generator of %s.%s is not type of QueryGenerator", dialect, structure.schema, structure.name)
		}
		queryGenerator = cached
		return
	}
	barrierKey := fmt.Sprintf("dialect_%s_%s_%s", dialect, structure.schema, structure.name)
	result, getErr, _ := gettingBarrier.Do(barrierKey, func() (v interface{}, doErr error) {
		builder, hasBuilder := getDialectQueryGeneratorBuilder(dialect)
		if !hasBuilder {
			doErr = fmt.Errorf("%s query generator builder of %s.%s is found", dialect, structure.schema, structure.name)
			return
		}
		generator, buildErr := builder.Build(structure)
		if buildErr != nil {
			doErr = buildErr
			return
		}
		structure.queryGenerators.Store(dialect, generator)
		v = generator
		return
	})
	gettingBarrier.Forget(barrierKey)
	if getErr != nil {
		err = getErr
		return
	}
	queryGenerator = result.(QueryGenerator)
	return
}

func (structure *ModelStructure) Type() (typ reflect.Type) {
	typ = structure.typ
	return
}

func (structure *ModelStructure) FindFieldByColumn(column string) (field *Field, has bool) {
	column = strings.ToUpper(strings.TrimSpace(column))
	for _, f := range structure.fields {
		for _, col := range f.columns {
			col = strings.ToUpper(strings.TrimSpace(col))
			if col == column {
				field = f
				has = true
				return
			}
		}
	}
	return
}

func (structure *ModelStructure) Copy() (v *ModelStructure) {
	orv := reflect.ValueOf(structure)
	ort := orv.Type()
	nv := reflect.NewAt(ort.Elem(), orv.UnsafePointer()).Elem().Interface().(ModelStructure)
	v = &nv
	v.queryGenerators = new(sync.Map)
	return
}

func (structure *ModelStructure) Name() (schema string, name string) {
	schema, name = structure.schema, structure.name
	return
}

func (structure *ModelStructure) Fields() (fields []*Field) {
	fields = structure.fields
	return
}

func (structure *ModelStructure) IncrPk() (v *Field, has bool) {
	if structure.fields == nil || len(structure.fields) == 0 {
		return
	}
	for _, field := range structure.fields {
		if field.IsIncrPk() {
			v = field
			has = true
			return
		}
	}
	return
}

func (structure *ModelStructure) Pk() (fields []*Field, has bool) {
	if structure.fields == nil || len(structure.fields) == 0 {
		return
	}
	fields = make([]*Field, 0, 1)
	for _, field := range structure.fields {
		if field.IsPk() {
			fields = append(fields, field)
			continue
		}
		if field.IsIncrPk() {
			fields = append(fields, field)
			continue
		}
	}
	has = len(fields) > 0
	return
}

func (structure *ModelStructure) AuditFields() (createBY *Field, createAT *Field, modifyBY *Field, modifyAT *Field, deleteBY *Field, deleteAT *Field, version *Field, has bool) {
	if structure.fields == nil || len(structure.fields) == 0 {
		return
	}
	n := 0
	for _, field := range structure.fields {
		if field.IsACB() {
			createBY = field
			has = true
			n++
		}
		if field.IsACT() {
			createAT = field
			has = true
			n++
		}
		if field.IsAMB() {
			modifyBY = field
			has = true
			n++
		}
		if field.IsAMT() {
			modifyAT = field
			has = true
			n++
		}
		if field.IsADB() {
			deleteBY = field
			has = true
			n++
		}
		if field.IsADT() {
			deleteAT = field
			n++
		}
		if field.IsAOL() {
			version = field
			has = true
			n++
		}
		if n > 6 {
			break
		}
	}
	return
}

func (structure *ModelStructure) scanReflectType(rt reflect.Type) (err error) {
	fieldNum := rt.NumField()
	for i := 0; i < fieldNum; i++ {
		sf := rt.Field(i)
		if !sf.IsExported() {
			continue
		}
		// anonymous
		if sf.Anonymous {
			if sf.Type.Kind() == reflect.Struct {
				anonymousRT := sf.Type
				err = structure.scanReflectType(anonymousRT)
				if err != nil {
					return
				}
			}
		}
		fieldName := sf.Name
		fieldTag, hasColTag := sf.Tag.Lookup(colTag)
		if !hasColTag {
			continue
		}
		fieldTag = strings.TrimSpace(fieldTag)
		if fieldTag == "" {
			err = fmt.Errorf("%s has col tag but no content", fieldName)
			return
		}
		if fieldTag == "-" {
			continue
		}
		err = structure.addField(sf)
		if err != nil {
			return
		}
	}
	return
}

func (structure *ModelStructure) addField(sf reflect.StructField) (err error) {
	fieldName := sf.Name
	fieldTag := sf.Tag.Get(colTag)
	fieldJson, hasJsonTag := sf.Tag.Lookup(jsonTag)
	if !hasJsonTag {
		fieldJson = fieldName
	}
	tagItems := strings.Split(fieldTag, ",")
	columnName := strings.TrimSpace(tagItems[0])
	// normal
	if len(tagItems) == 1 {
		field := &Field{
			kind:      normalKindField,
			conflict:  false,
			name:      fieldName,
			jsonName:  fieldJson,
			model:     structure,
			columns:   []string{columnName},
			reference: nil,
			link:      nil,
			virtual:   nil,
			tree:      nil,
		}
		structure.fields = append(structure.fields, field)
		return
	}
	kind := strings.ToUpper(strings.TrimSpace(tagItems[1]))
	conflicted := strings.Contains(kind, conflictKindField)
	if conflicted {
		if plusIdx := strings.Index(kind, "+"); plusIdx > 0 {
			kind = kind[0:plusIdx]
		} else {
			kind = normalKindField
		}
	}
	switch kind {
	case pkKindField:
		field := &Field{
			kind:      pkKindField,
			conflict:  false,
			name:      fieldName,
			jsonName:  fieldJson,
			model:     structure,
			columns:   []string{columnName},
			reference: nil,
			link:      nil,
			virtual:   nil,
			tree:      nil,
		}
		structure.fields = append(structure.fields, field)
		break
	case incrKindPkField:
		if !sf.Type.ConvertibleTo(reflect.TypeOf(int64(0))) {
			err = fmt.Errorf("%s has incr pk tag but type is not int64", fieldName)
			return
		}
		field := &Field{
			kind:      incrKindPkField,
			conflict:  false,
			name:      fieldName,
			jsonName:  fieldJson,
			model:     structure,
			columns:   []string{columnName},
			reference: nil,
			link:      nil,
			virtual:   nil,
			tree:      nil,
		}
		structure.fields = append(structure.fields, field)
		break
	case normalKindField:
		field := &Field{
			kind:      normalKindField,
			conflict:  conflicted,
			name:      fieldName,
			jsonName:  fieldJson,
			model:     structure,
			columns:   []string{columnName},
			reference: nil,
			link:      nil,
			virtual:   nil,
			tree:      nil,
		}
		structure.fields = append(structure.fields, field)
		break
	case jsonObjectKindField:
		field := &Field{
			kind:      jsonObjectKindField,
			conflict:  false,
			name:      fieldName,
			jsonName:  fieldJson,
			model:     structure,
			columns:   []string{columnName},
			reference: nil,
			link:      nil,
			virtual:   nil,
			tree:      nil,
		}
		structure.fields = append(structure.fields, field)
		break
	case acbKindField:
		if !(sf.Type.ConvertibleTo(reflect.TypeOf("")) || sf.Type.ConvertibleTo(reflect.TypeOf(int64(0)))) {
			err = fmt.Errorf("%s has acb tag but type is not int64 or string", fieldName)
			return
		}
		field := &Field{
			kind:      acbKindField,
			conflict:  false,
			name:      fieldName,
			jsonName:  fieldJson,
			model:     structure,
			columns:   []string{columnName},
			reference: nil,
			link:      nil,
			virtual:   nil,
			tree:      nil,
		}
		structure.fields = append(structure.fields, field)
		break
	case actKindField:
		if !sf.Type.ConvertibleTo(reflect.TypeOf(time.Time{})) {
			err = fmt.Errorf("%s has act tag but type is time.Time", fieldName)
			return
		}
		field := &Field{
			kind:      actKindField,
			conflict:  false,
			name:      fieldName,
			jsonName:  fieldJson,
			model:     structure,
			columns:   []string{columnName},
			reference: nil,
			link:      nil,
			virtual:   nil,
			tree:      nil,
		}
		structure.fields = append(structure.fields, field)
		break
	case ambKindField:
		if !(sf.Type.ConvertibleTo(reflect.TypeOf("")) || sf.Type.ConvertibleTo(reflect.TypeOf(int64(0)))) {
			err = fmt.Errorf("%s has amb tag but type is not int64 or string", fieldName)
			return
		}
		field := &Field{
			kind:      ambKindField,
			conflict:  false,
			name:      fieldName,
			jsonName:  fieldJson,
			model:     structure,
			columns:   []string{columnName},
			reference: nil,
			link:      nil,
			virtual:   nil,
			tree:      nil,
		}
		structure.fields = append(structure.fields, field)
		break
	case amtKindField:
		if !sf.Type.ConvertibleTo(reflect.TypeOf(time.Time{})) {
			err = fmt.Errorf("%s has amt tag but type is time.Time", fieldName)
			return
		}
		field := &Field{
			kind:      amtKindField,
			conflict:  false,
			name:      fieldName,
			jsonName:  fieldJson,
			model:     structure,
			columns:   []string{columnName},
			reference: nil,
			link:      nil,
			virtual:   nil,
			tree:      nil,
		}
		structure.fields = append(structure.fields, field)
		break
	case adbKindField:
		if !(sf.Type.ConvertibleTo(reflect.TypeOf("")) || sf.Type.ConvertibleTo(reflect.TypeOf(int64(0)))) {
			err = fmt.Errorf("%s has adb tag but type is not int64 or string", fieldName)
			return
		}
		field := &Field{
			kind:      adbKindField,
			conflict:  false,
			name:      fieldName,
			jsonName:  fieldJson,
			model:     structure,
			columns:   []string{columnName},
			reference: nil,
			link:      nil,
			virtual:   nil,
			tree:      nil,
		}
		structure.fields = append(structure.fields, field)
		break
	case adtKindField:
		if !sf.Type.ConvertibleTo(reflect.TypeOf(time.Time{})) {
			err = fmt.Errorf("%s has adt tag but type is time.Time", fieldName)
			return
		}
		field := &Field{
			kind:      adtKindField,
			conflict:  false,
			name:      fieldName,
			jsonName:  fieldJson,
			model:     structure,
			columns:   []string{columnName},
			reference: nil,
			link:      nil,
			virtual:   nil,
			tree:      nil,
		}
		structure.fields = append(structure.fields, field)
		break
	case aolKindField:
		if !sf.Type.ConvertibleTo(reflect.TypeOf(int64(0))) {
			err = fmt.Errorf("%s has aol tag but type is not int64", fieldName)
			return
		}
		field := &Field{
			kind:      aolKindField,
			conflict:  false,
			name:      fieldName,
			jsonName:  fieldJson,
			model:     structure,
			columns:   []string{columnName},
			reference: nil,
			link:      nil,
			virtual:   nil,
			tree:      nil,
		}
		structure.fields = append(structure.fields, field)
		break
	case virtualKindField:
		if len(tagItems) < 3 {
			err = fmt.Errorf("%s has virtual tag but source sql is not defined", fieldName)
			return
		}
		sourceSQL := strings.TrimSpace(strings.TrimSpace(tagItems[2]))
		field := &Field{
			kind:      virtualKindField,
			conflict:  false,
			name:      fieldName,
			jsonName:  fieldJson,
			model:     structure,
			columns:   []string{columnName},
			reference: nil,
			link:      nil,
			virtual: &VirtualField{
				name:  columnName,
				query: sourceSQL,
			},
			tree: nil,
		}
		structure.fields = append(structure.fields, field)
		break
	case referenceKindField:
		if !isModelType(sf.Type) {
			err = fmt.Errorf("%s has ref tag but type of field does not implement model", fieldName)
			return
		}
		if len(tagItems) != 3 {
			err = fmt.Errorf("%s has ref tag but refenerce is not defined", fieldName)
			return
		}
		refs := strings.Split(strings.TrimSpace(tagItems[2]), "+")
		if len(refs) != 2 {
			err = fmt.Errorf("%s has ref tag but definition of refenerce is not matched", fieldName)
			return
		}
		instance := newModelByType(sf.Type)
		ref, refErr := getModelStructure(instance)
		if refErr != nil {
			err = errors.Warning(fmt.Sprintf("%s has ref tag but get model structure failed", fieldName)).WithCause(refErr)
			return
		}
		srcColumns := scanReferenceOrLinkColumns(refs[0])
		targetModel := ref.Copy()
		targetFields := make([]*Field, 0, 1)
		targetColumns := scanReferenceOrLinkColumns(refs[1])
		if len(srcColumns) != len(targetColumns) {
			err = errors.Warning(fmt.Sprintf("%s has ref tag but definition of refenerce is not matched", fieldName))
			return
		}
		for _, column := range targetColumns {
			targetField, hasTargetField := targetModel.FindFieldByColumn(column)
			if !hasTargetField {
				err = errors.Warning(fmt.Sprintf("%s has ref tag can not find target field in model structure failed", fieldName))
				return
			}
			targetFields = append(targetFields, targetField)
		}
		field := &Field{
			kind:     referenceKindField,
			conflict: false,
			name:     fieldName,
			jsonName: fieldJson,
			model:    structure,
			columns:  srcColumns,
			reference: &ReferenceField{
				name:          columnName,
				targetModel:   targetModel,
				targetFields:  targetFields,
				targetColumns: targetColumns,
				abstracted:    false,
			},
			link:    nil,
			virtual: nil,
			tree:    nil,
		}
		structure.fields = append(structure.fields, field)
		break
	case linkKindField, linksKindField:
		linkType := sf.Type
		arrayed := kind == linksKindField
		if arrayed {
			if !(linkType.Kind() == reflect.Slice || linkType.Kind() == reflect.Array) {
				err = fmt.Errorf("%s has link(s) tag but field type is not slice", fieldName)
				return
			}
			linkType = linkType.Elem()
		}
		if !isModelType(linkType) {
			err = fmt.Errorf("%s has link(s) tag but type of field or element of field does not implement model", fieldName)
			return
		}
		if len(tagItems) < 3 {
			err = fmt.Errorf("%s has link(s) tag but definition is not defined", fieldName)
			return
		}
		refs := strings.Split(tagItems[2], "+")
		if len(refs) != 2 {
			err = fmt.Errorf("%s has link(s) tag but definition of refenerce is not matched", fieldName)
			return
		}
		srcColumns := scanReferenceOrLinkColumns(refs[0])
		targetColumns := scanReferenceOrLinkColumns(refs[1])
		if len(srcColumns) != len(targetColumns) {
			err = errors.Warning(fmt.Sprintf("%s has link(s) tag but definition of refenerce is not matched", fieldName))
			return
		}
		instance := newModelByType(linkType)
		link, linkErr := getModelStructure(instance)
		if linkErr != nil {
			err = errors.Warning(fmt.Sprintf("%s has link(s) tag but get model structure failed", fieldName)).WithCause(linkErr)
			return
		}
		field := &Field{
			kind:      kind,
			conflict:  false,
			name:      fieldName,
			jsonName:  fieldJson,
			model:     structure,
			columns:   srcColumns,
			reference: nil,
			link: &LinkField{
				name:          columnName,
				arrayed:       arrayed,
				targetModel:   link.Copy(),
				targetColumns: targetColumns,
				abstracted:    false,
				orders:        nil,
				rng:           nil,
			},
			virtual: nil,
			tree:    nil,
		}
		if len(tagItems) > 3 {
			settings := tagItems[2:]
			for _, setting := range settings {
				setting = strings.TrimSpace(setting)
				if ridx := strings.Index(setting, ":"); ridx > 0 {
					// range
					offset := strings.TrimSpace(setting[0:ridx])
					offset0, offsetErr := strconv.Atoi(offset)
					if offsetErr != nil {
						err = fmt.Errorf("%s is links, offset is not int", fieldName)
						return
					}
					limit := strings.TrimSpace(setting[ridx+1:])
					limit0, limitErr := strconv.Atoi(limit)
					if limitErr != nil {
						err = fmt.Errorf("%s is links, limit is not int", fieldName)
						return
					}
					field.link.rng = NewRange(offset0, limit0)
				} else {
					// orders
					field.link.orders = NewOrders()
					orders := strings.Split(setting, " ")
					if len(orders) == 1 {
						field.link.orders.Asc(setting)
					} else {
						orderField := orders[0]
						orderKind := ""
						for i := 1; i < len(orders); i++ {
							orderKind0 := orders[i]
							if orderKind0 == "" {
								continue
							}
							orderKind = strings.ToUpper(orderKind0)
							break
						}
						if orderKind == "" || orderKind == "ASC" {
							field.link.orders.Asc(orderField)
						} else {
							field.link.orders.Desc(orderField)
						}
					}
				}
			}
		}
		structure.fields = append(structure.fields, field)
		break
	case treeField:
		childrenFieldType := sf.Type
		if !(childrenFieldType.Kind() == reflect.Slice || childrenFieldType.Kind() == reflect.Array) {
			err = fmt.Errorf("%s has tree tag but field type is not slice", fieldName)
			return
		}
		childType := childrenFieldType.Elem()
		if fmt.Sprintf("%s.%s", structure.typ.PkgPath(), structure.typ.Name()) != fmt.Sprintf("%s.%s", childType.Elem().PkgPath(), childType.Elem().Name()) {
			err = fmt.Errorf("%s has tree tag but type of field or element of field is not same type", fieldName)
			return
		}
		if !isModelType(childrenFieldType.Elem()) {
			err = fmt.Errorf("%s has tree tag but type of field or element of field does not implement model", fieldName)
			return
		}
		if len(tagItems) != 3 {
			err = fmt.Errorf("%s has tree tag but definition is not defined", fieldName)
			return
		}
		refs := strings.Split(tagItems[2], "+")
		if len(refs) != 2 {
			err = fmt.Errorf("%s has tree tag but definition of refenerce is not matched", fieldName)
			return
		}
		nodeColumnName := strings.TrimSpace(refs[0])
		parentColumnName := strings.TrimSpace(refs[1])
		if parentColumnName == "" || nodeColumnName == "" {
			err = errors.Warning(fmt.Sprintf("%s has tree tag but definition of refenerce is not matched", fieldName))
			return
		}
		field := &Field{
			kind:      treeField,
			conflict:  false,
			name:      fieldName,
			jsonName:  fieldJson,
			model:     structure,
			columns:   []string{},
			reference: nil,
			link:      nil,
			virtual:   nil,
			tree: &TreeNodeField{
				nodeColumnName:   nodeColumnName,
				parentColumnName: parentColumnName,
			},
		}
		structure.fields = append(structure.fields, field)
		break
	default:
		err = fmt.Errorf("%s has col tag but kind is unknown", fieldName)
		return
	}
	return
}

func (structure *ModelStructure) scanAbstractedFields(rp *ModelStructureReferencePath) {
	for _, field := range structure.fields {
		if field.IsReference() {
			field.Reference().scanAbstracted(rp)
		} else if field.IsLink() {
			field.Link().scanAbstracted(rp)
		}
	}
	return
}
