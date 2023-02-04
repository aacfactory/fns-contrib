package dal

import (
	"context"
	db "database/sql"
	stdJson "encoding/json"
	"fmt"
	"github.com/aacfactory/errors"
	"github.com/aacfactory/fns-contrib/databases/sql"
	"github.com/aacfactory/json"
	"reflect"
	"strings"
	"time"
)

func QueryOne[T Model](ctx context.Context, conditions *Conditions) (result T, err errors.CodeError) {

	return
}

func Query[T Model](ctx context.Context, conditions *Conditions) (results []T, err errors.CodeError) {

	return
}

func QueryWithRange[T Model](ctx context.Context, conditions *Conditions, orders *Orders, rng *Range) (results []T, err errors.CodeError) {

	return
}

func query0[T Model](ctx context.Context, conditions *Conditions, orders *Orders, rng *Range) (results []T, err errors.CodeError) {
	model := newModel[T]()
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
	results, err = scanQueryResults[T](ctx, rows)
	if err != nil {
		return
	}
	if results == nil || len(results) == 0 {
		return
	}
	// todo eager load mode
	/*
		for results -> 取出[]{[]querys（IN）}
		然后 for list，查数据库，结果在set进去
	*/
	if isEagerLoadMode(ctx) {

		for _, result := range results {

		}
		fields := structure.Fields()
		for _, field := range fields {
			if field.IsReference() {
				reference := field.Reference()
				if reference.Abstracted() {
					reference.targetModel
				}
			}
		}
	}
	return
}

func scanQueryResults[T Model](ctx context.Context, rows sql.Rows) (results []T, err errors.CodeError) {
	results = make([]T, 0, 1)
	for {
		row, has := rows.Next()
		if !has {
			break
		}
		result, scanErr := scanQueryResult[T](ctx, row)
		if err != nil {
			err = scanErr
			return
		}
		results = append(results, result)
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

func scanQueryResult[T Model](ctx context.Context, row sql.Row) (result T, err errors.CodeError) {
	result = newModel[T]()
	prv := reflect.ValueOf(result)
	rv := prv.Elem()
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
	hookErr := executeModelLoadHook(ctx, prv)
	if hookErr != nil {
		err = hookErr
	}
	return
}
