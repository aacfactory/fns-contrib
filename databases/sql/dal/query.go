package dal

import (
	"context"
	stdJson "encoding/json"
	"github.com/aacfactory/copier"
	"github.com/aacfactory/errors"
	"github.com/aacfactory/fns-contrib/databases/sql"
	"github.com/aacfactory/json"
	"reflect"
	"strings"
)

func QueryOne[T Model](ctx context.Context, conditions *Conditions) (result T, err errors.CodeError) {
	results := make([]T, 0, 1)
	queryErr := query0(ctx, conditions, nil, nil, &result)
	if queryErr != nil {
		err = errors.ServiceError("dal: query one failed").WithCause(queryErr)
		return
	}
	if len(results) == 0 {
		return
	}
	result = results[0]
	return
}

func Query[T Model](ctx context.Context, conditions *Conditions) (results []T, err errors.CodeError) {
	results = make([]T, 0, 1)
	err = query0(ctx, conditions, nil, nil, &results)
	if err != nil {
		err = errors.ServiceError("dal: query failed").WithCause(err)
		return
	}
	return
}

func QueryWithRange[T Model](ctx context.Context, conditions *Conditions, orders *Orders, rng *Range) (results []T, err errors.CodeError) {
	results = make([]T, 0, 1)
	err = query0(ctx, conditions, orders, rng, &results)
	if err != nil {
		err = errors.ServiceError("dal: query with range failed").WithCause(err)
		return
	}
	return
}

func QueryDirect[T Model](ctx context.Context, query string, args ...interface{}) (results []T, err errors.CodeError) {
	rows, queryErr := sql.Query(ctx, query, args...)
	if queryErr != nil {
		err = errors.ServiceError("dal: query direct failed").WithCause(queryErr)
		return
	}
	if rows.Empty() {
		return
	}
	results = make([]T, 0, 1)
	resultsPtrValue := reflect.ValueOf(&results)
	err = scanQueryResults(ctx, rows, resultsPtrValue)
	if err != nil {
		err = errors.ServiceError("dal: query direct failed").WithCause(err)
		return
	}
	if results == nil || len(results) == 0 {
		return
	}
	structure, _, getGeneratorErr := getModelQueryGenerator(ctx, newModel[T]())
	if getGeneratorErr != nil {
		err = errors.ServiceError("dal: query direct failed").WithCause(err).WithCause(getGeneratorErr)
		return
	}
	tryHandleEagerLoadErr := tryHandleEagerLoad(ctx, structure, resultsPtrValue)
	if tryHandleEagerLoadErr != nil {
		err = tryHandleEagerLoadErr
		return
	}
	return
}

func query0(ctx context.Context, conditions *Conditions, orders *Orders, rng *Range, resultsPtr interface{}) (err errors.CodeError) {
	resultsPtrValue := reflect.ValueOf(resultsPtr)
	resultPtrValue := reflect.New(resultsPtrValue.Type().Elem().Elem())
	model := resultPtrValue.Interface().(Model)
	structure, generator, getGeneratorErr := getModelQueryGenerator(ctx, model)
	if getGeneratorErr != nil {
		err = getGeneratorErr
		return
	}
	// generator
	_, query, arguments, generateErr := generator.Query(ctx, conditions, orders, rng)
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
	if rows.Empty() {
		return
	}
	scanErr := scanQueryResults(ctx, rows, resultsPtrValue)
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

func tryHandleEagerLoad(ctx context.Context, structure *ModelStructure, resultsPtrValue reflect.Value) (err errors.CodeError) {
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
			err = errors.ServiceError("eager load failed").WithCause(loadErr).WithMeta("field", fieldName)
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
					err = errors.ServiceError("eager load failed").WithCause(cpErr).WithMeta("field", fieldName)
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
						err = errors.ServiceError("eager load failed").WithCause(cpErr).WithMeta("field", fieldName)
					}
				}
			}
		}
	}
	return
}

var (
	sqlStringType = reflect.TypeOf("")
	sqlIntType    = reflect.TypeOf(int64(0))
	sqlFloatType  = reflect.TypeOf(float64(0))
	sqlBoolType   = reflect.TypeOf(false)
)

func scanQueryResults(ctx context.Context, rows sql.Rows, resultsPtrValue reflect.Value) (err errors.CodeError) {
	resultsValue := resultsPtrValue.Elem()
	for {
		row, has := rows.Next()
		if !has {
			break
		}
		resultPtrValue := reflect.New(resultsValue.Type().Elem().Elem())
		scanErr := scanQueryResult(ctx, row, resultPtrValue)
		if err != nil {
			err = scanErr
			return
		}
		resultsValue = reflect.Append(resultsValue, resultPtrValue)
	}
	return
}

func scanQueryResult(ctx context.Context, row sql.Row, resultPtrValue reflect.Value) (err errors.CodeError) {
	rv := resultPtrValue.Elem()
	rt := rv.Type()
	fieldNum := rt.NumField()
	columns := row.Columns()
	for _, c := range columns {
		if c.IsNil() {
			continue
		}
		cName := strings.ToUpper(strings.TrimSpace(c.Name()))
		field := reflect.StructField{}
		hasField := false
		jsonValueField := false
		for i := 0; i < fieldNum; i++ {
			structField := rt.Field(i)
			tagValue, hasTag := structField.Tag.Lookup(tag)
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
				jsonValueField = strings.Contains(tagValue, "REF") ||
					strings.Contains(tagValue, "LINK") ||
					strings.Contains(tagValue, "LINKS") ||
					strings.Contains(tagValue, "VC") ||
					strings.Contains(tagValue, "TREE")
				break
			}
		}
		if !hasField {
			continue
		}
		rfv := rv.FieldByName(field.Name)
		value, valueErr := c.Value()
		if valueErr != nil {
			err = errors.Warning("sql: scan query result failed").
				WithMeta("column", cName).
				WithCause(valueErr)
			return
		}
		reflectValue := reflect.ValueOf(value)
		if jsonValueField {
			var jsonRaw []byte
			switch value.(type) {
			case []byte:
				jsonRaw = value.([]byte)
				break
			case json.RawMessage:
				jsonRaw = value.(json.RawMessage)
				break
			case stdJson.RawMessage:
				jsonRaw = value.(stdJson.RawMessage)
				break
			default:
				err = errors.Warning("sql: scan query result failed").
					WithMeta("column", cName).
					WithCause(errors.Warning("sql: column value type is not json bytes"))
				return
			}
			var jsonValue reflect.Value
			if field.Type.Kind() == reflect.Ptr {
				jsonValue = reflect.New(field.Type.Elem())
			} else {
				jsonValue = reflect.New(field.Type)
			}
			decodeErr := json.Unmarshal(jsonRaw, jsonValue.Interface())
			if decodeErr != nil {
				err = errors.Warning("sql: scan query result failed").
					WithMeta("column", cName).
					WithCause(decodeErr)
				return
			}
			rfv.Set(jsonValue)
		} else if rfv.CanSet() {
			if reflectValue.Type() == field.Type || reflectValue.Type().AssignableTo(field.Type) {
				rv.FieldByName(field.Name).Set(reflectValue)
			} else if reflectValue.Type().ConvertibleTo(field.Type) {
				rfv.Set(reflectValue.Convert(field.Type))
			} else {
				err = errors.Warning("sql: scan query result failed").
					WithMeta("column", cName).
					WithCause(errors.Warning("sql: column value type can match row field type").WithMeta("field", field.Name))
				return
			}
		} else if field.Type == sqlStringType || field.Type.ConvertibleTo(sqlStringType) {
			if reflectValue.Type() == sqlStringType {
				rfv.SetString(reflectValue.String())
			} else if reflectValue.Type().ConvertibleTo(sqlStringType) {
				rfv.SetString(reflectValue.Convert(sqlStringType).String())
			} else {
				err = errors.Warning("sql: scan query result failed").
					WithMeta("column", cName).
					WithCause(errors.Warning("sql: column value type can match row field type").WithMeta("field", field.Name))
				return
			}
		} else if field.Type == sqlBoolType || field.Type.ConvertibleTo(sqlBoolType) {
			if reflectValue.Type() == sqlBoolType {
				rfv.SetBool(reflectValue.Bool())
			} else if reflectValue.Type().ConvertibleTo(sqlBoolType) {
				rfv.SetBool(reflectValue.Convert(sqlBoolType).Bool())
			} else {
				err = errors.Warning("sql: scan query result failed").
					WithMeta("column", cName).
					WithCause(errors.Warning("sql: column value type can match row field type").WithMeta("field", field.Name))
				return
			}
		} else if field.Type == sqlIntType || field.Type.ConvertibleTo(sqlIntType) {
			if reflectValue.Type() == sqlIntType {
				rfv.SetInt(reflectValue.Int())
			} else if reflectValue.Type().ConvertibleTo(sqlIntType) {
				rfv.SetInt(reflectValue.Convert(sqlIntType).Int())
			} else {
				err = errors.Warning("sql: scan query result failed").
					WithMeta("column", cName).
					WithCause(errors.Warning("sql: column value type can match row field type").WithMeta("field", field.Name))
				return
			}
		} else if field.Type == sqlFloatType || field.Type.ConvertibleTo(sqlFloatType) {
			if reflectValue.Type() == sqlFloatType {
				rfv.SetFloat(reflectValue.Float())
			} else if reflectValue.Type().ConvertibleTo(sqlFloatType) {
				rfv.SetFloat(reflectValue.Convert(sqlFloatType).Float())
			} else {
				err = errors.Warning("sql: scan query result failed").
					WithMeta("column", cName).
					WithCause(errors.Warning("sql: column value type can match row field type").WithMeta("field", field.Name))
				return
			}
		} else {
			err = errors.Warning("sql: scan query result failed").
				WithMeta("column", cName).
				WithCause(errors.Warning("sql: field type was not supported").WithMeta("field", field.Name))
			return
		}
	}
	// load hook
	hookErr := executeModelLoadHook(ctx, resultPtrValue)
	if hookErr != nil {
		err = hookErr
	}
	return
}
