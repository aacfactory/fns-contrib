package dal

import (
	"fmt"
	"github.com/aacfactory/copier"
	"github.com/aacfactory/errors"
	"github.com/aacfactory/fns-contrib/databases/sql"
	"github.com/aacfactory/fns/context"
	"github.com/aacfactory/json"
	"reflect"
	"strings"
)

func QueryOne[T Model](ctx context.Context, conditions *Conditions) (result T, err error) {
	results := make([]T, 0, 1)
	queryErr := query0(ctx, conditions, nil, nil, &result)
	if queryErr != nil {
		err = errors.Warning("dal: query one failed").WithCause(queryErr)
		return
	}
	if len(results) == 0 {
		return
	}
	result = results[0]
	return
}

func Query[T Model](ctx context.Context, conditions *Conditions) (results []T, err error) {
	results = make([]T, 0, 1)
	err = query0(ctx, conditions, nil, nil, &results)
	if err != nil {
		err = errors.Warning("dal: query failed").WithCause(err)
		return
	}
	return
}

func QueryWithRange[T Model](ctx context.Context, conditions *Conditions, orders *Orders, rng *Range) (results []T, err error) {
	results = make([]T, 0, 1)
	err = query0(ctx, conditions, orders, rng, &results)
	if err != nil {
		err = errors.Warning("dal: query with range failed").WithCause(err)
		return
	}
	return
}

func QueryDirect[T Model](ctx context.Context, query string, args ...interface{}) (results []T, err error) {
	rows, queryErr := sql.Query(ctx, query, args...)
	if queryErr != nil {
		err = errors.Warning("dal: query direct failed").WithCause(queryErr)
		return
	}
	results = make([]T, 0, 1)
	resultsPtrValue := reflect.ValueOf(&results)
	for rows.Next() {
		err = scanQueryResults(ctx, rows, resultsPtrValue)
		if err != nil {
			err = errors.Warning("dal: query direct failed").WithCause(err)
			return
		}
	}
	_ = rows.Close()
	if results == nil || len(results) == 0 {
		return
	}
	structure, _, getGeneratorErr := getModelQueryGenerator(ctx, newModel[T]())
	if getGeneratorErr != nil {
		err = errors.Warning("dal: query direct failed").WithCause(err).WithCause(getGeneratorErr)
		return
	}
	tryHandleEagerLoadErr := tryHandleEagerLoad(ctx, structure, resultsPtrValue)
	if tryHandleEagerLoadErr != nil {
		err = tryHandleEagerLoadErr
		return
	}
	return
}

func query0(ctx context.Context, conditions *Conditions, orders *Orders, rng *Range, resultsPtr interface{}) (err error) {
	resultsPtrValue := reflect.ValueOf(resultsPtr)
	resultPtrValue := reflect.New(resultsPtrValue.Elem().Type().Elem().Elem())
	model := resultPtrValue.Interface().(Model)
	structure, generator, getGeneratorErr := getModelQueryGenerator(ctx, model)
	if getGeneratorErr != nil {
		err = getGeneratorErr
		return
	}
	// generator
	_, query, arguments, generateErr := generator.Select(ctx, conditions, orders, rng)
	if generateErr != nil {
		err = errors.Map(generateErr)
		return
	}
	// handle
	rows, queryErr := sql.Query(ctx, query, arguments...)
	if queryErr != nil {
		err = queryErr
		return
	}
	scanErr := scanQueryResults(ctx, rows, resultsPtrValue)
	_ = rows.Close()
	if scanErr != nil {
		err = scanErr
		return
	}
	if resultsPtrValue.Elem().Len() == 0 {
		return
	}
	tryHandleEagerLoadErr := tryHandleEagerLoad(ctx, structure, resultsPtrValue)
	if tryHandleEagerLoadErr != nil {
		err = tryHandleEagerLoadErr
		return
	}
	return
}

func tryHandleEagerLoad(ctx context.Context, structure *ModelStructure, resultsPtrValue reflect.Value) (err error) {
	if !isEagerLoadMode(ctx) {
		return
	}
	resultsValue := resultsPtrValue.Elem()
	resultsValueLen := resultsValue.Len()
	for i := 0; i < resultsValueLen; i++ {

	}
	eagerLoaders := make(map[string]*EagerLoader)
	for i := 0; i < resultsValueLen; i++ {
		resultValue := resultsValue.Index(i)
		fields := structure.Fields()
		for _, field := range fields {
			if field.IsReference() && field.reference != nil && field.reference.abstracted {
				eagerLoader, hasEagerLoader := eagerLoaders[field.Name()]
				if !hasEagerLoader {
					eagerLoader0, newEagerLoaderErr := newEagerLoader(field.reference.targetModel)
					if newEagerLoaderErr != nil {
						err = newEagerLoaderErr
						return
					}
					eagerLoader = eagerLoader0
					eagerLoaders[field.Name()] = eagerLoader
				}
				refField := resultValue.Elem().FieldByName(field.Name())
				if refField.IsNil() {
					continue
				}
				refPkValue := refField.Elem().FieldByName(eagerLoader.pk.Name()).Interface()
				eagerLoader.AppendKey(refPkValue)
				continue
			}
			if field.IsLink() && field.link != nil && field.link.abstracted {
				eagerLoader, hasEagerLoader := eagerLoaders[field.Name()]
				if !hasEagerLoader {
					eagerLoader0, newEagerLoaderErr := newEagerLoader(field.link.targetModel)
					if newEagerLoaderErr != nil {
						err = newEagerLoaderErr
						return
					}
					eagerLoader = eagerLoader0
					eagerLoaders[field.Name()] = eagerLoader
				}
				if field.link.arrayed {
					linkField := resultValue.Elem().FieldByName(field.Name())
					if linkField.IsNil() {
						continue
					}
					linkFieldValueLen := linkField.Len()
					if linkFieldValueLen == 0 {
						continue
					}

					for i := 0; i < linkFieldValueLen; i++ {
						linkPkValue := linkField.Index(i).Elem().FieldByName(eagerLoader.pk.Name()).Interface()
						eagerLoader.AppendKey(linkPkValue)
					}
				} else {
					linkField := resultValue.Elem().FieldByName(field.Name())
					if linkField.IsNil() {
						continue
					}
					linkPkValue := linkField.Elem().FieldByName(eagerLoader.pk.Name()).Interface()
					eagerLoader.AppendKey(linkPkValue)
				}
			}
		}
	}
	if len(eagerLoaders) == 0 {
		return
	}
	for fieldName, loader := range eagerLoaders {
		loaded, eagerLoadValues, loadErr := loader.Load(ctx)
		if loadErr != nil {
			err = errors.Warning("eager load failed").WithCause(loadErr).WithMeta("field", fieldName)
			return
		}
		if !loaded {
			continue
		}
		for i := 0; i < resultsValueLen; i++ {
			resultValue := resultsValue.Index(i)
			rf := resultValue.Elem().FieldByName(fieldName)
			if rf.Kind() == reflect.Ptr {
				if rf.IsNil() {
					continue
				}
				pkv := rf.Elem().FieldByName(loader.pk.Name()).Interface()
				eagerLoadValue, hasEagerLoadValue := eagerLoadValues[pkv]
				if !hasEagerLoadValue {
					continue
				}
				cpErr := copier.Copy(rf.Interface(), eagerLoadValue)
				if cpErr != nil {
					err = errors.Warning("eager load failed").WithCause(cpErr).WithMeta("field", fieldName)
				}
			} else {
				// slice
				if rf.IsNil() {
					continue
				}
				rfLen := rf.Len()
				if rfLen == 0 {
					continue
				}
				for i := 0; i < rfLen; i++ {
					rfe := rf.Index(i)
					pkv := rfe.Elem().FieldByName(loader.pk.Name()).Interface()
					eagerLoadValue, hasEagerLoadValue := eagerLoadValues[pkv]
					if !hasEagerLoadValue {
						continue
					}
					cpErr := copier.Copy(rfe.Interface(), eagerLoadValue)
					if cpErr != nil {
						err = errors.Warning("eager load failed").WithCause(cpErr).WithMeta("field", fieldName)
					}
				}
			}
		}
	}
	return
}

func scanQueryResults(ctx context.Context, rows *sql.Rows, resultsPtrValue reflect.Value) (err error) {
	resultsValue := resultsPtrValue.Elem()
	for rows.Next() {
		resultPtrValue := reflect.New(resultsValue.Type().Elem().Elem())
		scanErr := scanQueryResult(ctx, rows, resultPtrValue)
		if scanErr != nil {
			err = scanErr
			return
		}
		resultsValue = reflect.Append(resultsValue, resultPtrValue)
	}
	resultsPtrValue.Elem().Set(resultsValue)
	return
}

func scanQueryResult(ctx context.Context, rows *sql.Rows, resultPtrValue reflect.Value) (err error) {
	rv := resultPtrValue.Elem()
	rt := rv.Type()
	fieldNum := rt.NumField()

	dst := make([]interface{}, 0, 1)
	jsonFields := make([]int, 0, 1)
	valueFields := make([]int, 0, 1)
	columns := rows.Columns()
	for idx, c := range columns {
		cName := strings.ToUpper(strings.TrimSpace(c.Name))
		field := reflect.StructField{}
		hasField := false
		jsonValueField := false
		for i := 0; i < fieldNum; i++ {
			structField := rt.Field(i)
			tagValue, hasTag := structField.Tag.Lookup(colTag)
			if !hasTag {
				continue
			}
			columnName := ""
			settingIdx := strings.Index(tagValue, ",")
			if settingIdx > 0 {
				columnName = tagValue[0:settingIdx]
			} else {
				columnName = tagValue
			}
			columnName = strings.ToUpper(strings.TrimSpace(columnName))
			if columnName == "-" {
				continue
			}
			if columnName == cName {
				field = structField
				hasField = true
				// "REF", "LINK", "LINKS", "VC", "TREE"
				tagValue = strings.ToUpper(tagValue)
				jsonValueField = strings.Contains(tagValue, "REF") ||
					strings.Contains(tagValue, "LINK") ||
					strings.Contains(tagValue, "LINKS") ||
					//strings.Contains(tagValue, "VC") ||
					strings.Contains(tagValue, "TREE")
				break
			}
		}
		if !hasField {
			continue
		}
		if jsonValueField {
			dst = append(dst, &json.RawMessage{})
			jsonFields = append(jsonFields, idx)
			valueFields = append(valueFields, idx)
			continue
		}
		rfv := rv.FieldByName(field.Name)
		if rfv.Type().Kind() == reflect.Ptr {
			dst = append(dst, rfv.Interface())
			continue
		}
		if !rfv.CanInterface() {
			err = errors.Warning("sql: scan query result failed").
				WithMeta("column", cName).
				WithCause(fmt.Errorf("value can not interface"))
			return
		}
		rvi := rfv.Interface()
		dst = append(dst, &rvi)
		valueFields = append(valueFields, idx)
	}
	scanErr := rows.Scan(dst...)
	if scanErr != nil {
		err = errors.Warning("sql: scan query result failed").
			WithCause(scanErr)
		return
	}
	for _, idx := range valueFields {
		value := dst[idx]
		isJson := false
		for _, jf := range jsonFields {
			if jf == idx {
				isJson = true
				break
			}
		}
		field := rv.Field(idx)
		if isJson {
			p := value.(*json.RawMessage)
			if field.Type().Kind() == reflect.Ptr {
				fv := field.Interface()
				decodeErr := json.Unmarshal(*p, fv)
				if decodeErr != nil {
					err = errors.Warning("sql: scan query result failed").
						WithCause(decodeErr)
					return
				}
			} else {
				fv := field.Interface()
				decodeErr := json.Unmarshal(*p, &fv)
				if decodeErr != nil {
					err = errors.Warning("sql: scan query result failed").
						WithCause(decodeErr)
					return
				}
				field.Set(reflect.ValueOf(fv))
			}
			continue
		}
		vv := reflect.Indirect(reflect.ValueOf(value))
		field.Set(vv)
	}
	// load hook
	hookErr := executeModelLoadHook(ctx, resultPtrValue)
	if hookErr != nil {
		err = hookErr
	}
	return
}
