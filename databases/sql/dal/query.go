package dal

import (
	"context"
	db "database/sql"
	stdJson "encoding/json"
	"fmt"
	"github.com/aacfactory/copier"
	"github.com/aacfactory/errors"
	"github.com/aacfactory/fns-contrib/databases/sql"
	"github.com/aacfactory/json"
	"reflect"
	"strings"
	"time"
)

func QueryOne[T Model](ctx context.Context, conditions *Conditions) (result T, err errors.CodeError) {
	results := make([]T, 0, 1)
	queryErr := query0[T](ctx, conditions, nil, nil, &result)
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
	err = query0[T](ctx, conditions, nil, nil, &results)
	if err != nil {
		err = errors.ServiceError("dal: query failed").WithCause(err)
		return
	}
	return
}

func QueryWithRange[T Model](ctx context.Context, conditions *Conditions, orders *Orders, rng *Range) (results []T, err errors.CodeError) {
	results = make([]T, 0, 1)
	err = query0[T](ctx, conditions, orders, rng, &results)
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
	sqlNullStringType  = reflect.TypeOf(db.NullString{})
	sqlNullInt16Type   = reflect.TypeOf(db.NullInt16{})
	sqlNullInt32Type   = reflect.TypeOf(db.NullInt32{})
	sqlNullInt64Type   = reflect.TypeOf(db.NullInt64{})
	sqlNullFloat64Type = reflect.TypeOf(db.NullFloat64{})
	sqlNullBoolType    = reflect.TypeOf(db.NullBool{})
	sqlNullTimeType    = reflect.TypeOf(db.NullTime{})
	sqlBytesType       = reflect.TypeOf([]byte{})
	sqlJsonType        = reflect.TypeOf(json.RawMessage{})
	sqlSTDJsonType     = reflect.TypeOf(stdJson.RawMessage{})
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
				break
			}
		}
		if !hasField {
			continue
		}
		switch sql.ColumnType(c.Type()) {
		case sql.StringType:
			v := ""
			decodeErr := c.Get(&v)
			if decodeErr != nil {
				err = errors.Warning(fmt.Sprintf("get %s failed", cName)).WithCause(decodeErr)
				return
			}
			if field.Type == sqlNullStringType {
				vv := db.NullString{
					String: v,
					Valid:  true,
				}
				rv.FieldByName(field.Name).Set(reflect.ValueOf(vv))
			} else {
				rv.FieldByName(field.Name).SetString(v)
			}
			break
		case sql.BoolType:
			v := false
			decodeErr := c.Get(&v)
			if decodeErr != nil {
				err = errors.Warning(fmt.Sprintf("get %s failed", cName)).WithCause(decodeErr)
				return
			}
			if field.Type == sqlNullBoolType {
				vv := db.NullBool{
					Bool:  v,
					Valid: true,
				}
				rv.FieldByName(field.Name).Set(reflect.ValueOf(vv))
			} else {
				rv.FieldByName(field.Name).SetBool(v)
			}
			break
		case sql.IntType:
			v := int64(0)
			decodeErr := c.Get(&v)
			if decodeErr != nil {
				err = errors.Warning(fmt.Sprintf("get %s failed", cName)).WithCause(decodeErr)
				return
			}
			if field.Type == sqlNullInt16Type {
				vv := db.NullInt16{
					Int16: int16(v),
					Valid: true,
				}
				rv.FieldByName(field.Name).Set(reflect.ValueOf(vv))
			} else if field.Type == sqlNullInt32Type {
				vv := db.NullInt32{
					Int32: int32(v),
					Valid: true,
				}
				rv.FieldByName(field.Name).Set(reflect.ValueOf(vv))
			} else if field.Type == sqlNullInt64Type {
				vv := db.NullInt64{
					Int64: v,
					Valid: true,
				}
				rv.FieldByName(field.Name).Set(reflect.ValueOf(vv))
			} else {
				rv.FieldByName(field.Name).SetInt(v)
			}
			break
		case sql.FloatType:
			v := 0.0
			decodeErr := c.Get(&v)
			if decodeErr != nil {
				err = errors.Warning(fmt.Sprintf("get %s failed", cName)).WithCause(decodeErr)
				return
			}
			if field.Type == sqlNullFloat64Type {
				vv := db.NullFloat64{
					Float64: v,
					Valid:   true,
				}
				rv.FieldByName(field.Name).Set(reflect.ValueOf(vv))
			} else {
				rv.FieldByName(field.Name).SetFloat(v)
			}
			break
		case sql.DatetimeType:
			v := time.Time{}
			decodeErr := c.Get(&v)
			if decodeErr != nil {
				err = errors.Warning(fmt.Sprintf("get %s failed", cName)).WithCause(decodeErr)
				return
			}
			if field.Type == sqlNullTimeType {
				vv := db.NullTime{
					Time:  v,
					Valid: true,
				}
				rv.FieldByName(field.Name).Set(reflect.ValueOf(vv))
			} else {
				rv.FieldByName(field.Name).Set(reflect.ValueOf(v).Convert(field.Type))
			}
			break
		case sql.BytesType:
			rv.FieldByName(field.Name).SetBytes(c.RawValue())
			break
		case sql.TimeType:
			v := sql.Time{}
			decodeErr := c.Get(&v)
			if decodeErr != nil {
				err = errors.Warning(fmt.Sprintf("get %s failed", cName)).WithCause(decodeErr)
				return
			}
			rv.FieldByName(field.Name).Set(reflect.ValueOf(v).Convert(field.Type))
			break
		case sql.JsonType:
			if field.Type == sqlJsonType || field.Type == sqlSTDJsonType {
				rv.FieldByName(field.Name).Set(reflect.ValueOf(c.RawValue()).Convert(field.Type))
			} else {
				v := reflect.New(field.Type).Interface()
				decodeErr := c.Get(&v)
				if decodeErr != nil {
					err = errors.Warning(fmt.Sprintf("get %s failed", cName)).WithCause(decodeErr)
					return
				}
				rv.FieldByName(field.Name).Set(reflect.ValueOf(v).Elem())
			}
			break
		case sql.DateType:
			v := sql.Date{}
			decodeErr := c.Get(&v)
			if decodeErr != nil {
				err = errors.Warning(fmt.Sprintf("get %s failed", cName)).WithCause(decodeErr)
				return
			}
			rv.FieldByName(field.Name).Set(reflect.ValueOf(v).Convert(field.Type))
			break
		case sql.UnknownType:
			if field.Type.AssignableTo(sqlBytesType) {
				rv.FieldByName(field.Name).SetBytes(c.RawValue())
			}
		}
	}
	// load hook
	hookErr := executeModelLoadHook(ctx, resultPtrValue)
	if hookErr != nil {
		err = hookErr
	}
	return
}
