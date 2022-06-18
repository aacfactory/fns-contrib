package mysql

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

func Query(ctx context.Context, cond *Conditions, rows interface{}) (fetched bool, err errors.CodeError) {
	fetched0, queryErr := query0(ctx, cond, nil, nil, rows)
	if queryErr != nil {
		err = errors.ServiceError("mysql: query failed").WithCause(queryErr).WithMeta("mysql", "query")
		return
	}
	fetched = fetched0
	return
}

func QueryWithRange(ctx context.Context, cond *Conditions, orders *Orders, rng *Range, rows interface{}) (fetched bool, err errors.CodeError) {
	fetched0, queryErr := query0(ctx, cond, orders, rng, rows)
	if queryErr != nil {
		err = errors.ServiceError("mysql: query with range failed").WithCause(queryErr).WithMeta("mysql", "query with range")
		return
	}
	fetched = fetched0
	return
}

func QueryOne(ctx context.Context, cond *Conditions, row interface{}) (fetched bool, err errors.CodeError) {
	if row == nil {
		err = errors.ServiceError("mysql: query one failed for row is nil").WithMeta("mysql", "one")
		return
	}
	rv := reflect.ValueOf(row)
	if rv.Type().Kind() != reflect.Ptr {
		err = errors.ServiceError("mysql: query one failed for type of row is not ptr").WithMeta("mysql", "one")
		return
	}
	if rv.Elem().Type().Kind() != reflect.Struct {
		err = errors.ServiceError("mysql: query one failed for type of row is not ptr struct").WithMeta("mysql", "one")
		return
	}
	rowsRV := reflect.New(reflect.SliceOf(reflect.TypeOf(row)))
	rows := rowsRV.Interface()
	fetched0, queryErr := query0(ctx, cond, nil, nil, rows)
	if queryErr != nil {
		err = errors.ServiceError("mysql: query one failed").WithCause(queryErr).WithMeta("mysql", "one")
		return
	}
	if !fetched0 {
		return
	}
	fetched = fetched0
	rv.Elem().Set(rowsRV.Elem().Index(0).Elem())
	return
}

func QueryDirect(ctx context.Context, rows interface{}, query string, args ...interface{}) (fetched bool, err error) {
	// query
	results, queryErr := sql.Query(ctx, query, args...)
	if queryErr != nil {
		err = errors.ServiceError("mysql: query direct failed").WithCause(queryErr).WithMeta("mysql", "query direct")
		return
	}
	fetched = !results.Empty()
	if !fetched {
		return
	}
	rv := reflect.ValueOf(rows)
	scanErr := scanQueryResults(ctx, results, rv)
	if scanErr != nil {
		err = errors.ServiceError("mysql: query direct failed").WithCause(scanErr).WithMeta("mysql", "query direct")
		return
	}
	return
}

func query0(ctx context.Context, cond *Conditions, orders *Orders, rng *Range, rows interface{}) (fetched bool, err error) {
	if rows == nil {
		err = fmt.Errorf("rows is nil")
		return
	}
	rv := reflect.ValueOf(rows)
	if rv.Type().Kind() != reflect.Ptr {
		err = fmt.Errorf("type of rows is not ptr")
		return
	}
	if rv.Elem().Type().Kind() != reflect.Slice {
		err = fmt.Errorf("type of rows is not ptr slict")
		return
	}
	if rv.Elem().Type().Elem().Kind() != reflect.Ptr {
		err = fmt.Errorf("rows element type is not ptr")
		return
	}

	rowTmp := reflect.New(rv.Elem().Type().Elem().Elem()).Interface()
	tab := createOrLoadTable(rowTmp)

	var orderValues []*Order = nil
	if orders != nil {
		orderValues = orders.values
	}
	query, args := tab.generateQuerySQL(cond, rng, orderValues)
	// query
	results, queryErr := sql.Query(ctx, query, args...)
	if queryErr != nil {
		err = queryErr
		return
	}
	fetched = !results.Empty()
	if !fetched {
		return
	}

	scanErr := scanQueryResults(ctx, results, rv)
	if scanErr != nil {
		err = scanErr
		return
	}
	return
}

func scanQueryResults(ctx context.Context, results sql.Rows, rows reflect.Value) (err error) {
	rv := rows.Elem()
	for {
		result, has := results.Next()
		if !has {
			break
		}
		row := reflect.New(rv.Type().Elem().Elem())
		err = scanQueryResult(ctx, result, row)
		if err != nil {
			return
		}
		rv = reflect.Append(rv, row)
	}
	rows.Elem().Set(rv)
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

func scanQueryResult(ctx context.Context, result sql.Row, row reflect.Value) (err error) {
	rv := row.Elem()
	rt := rv.Type()
	fieldNum := rt.NumField()
	columns := result.Columns()
	for _, c := range columns {
		if c.IsNil() {
			continue
		}
		cName := strings.ToUpper(strings.TrimSpace(c.Name()))
		field := reflect.StructField{}
		hasField := false
		for i := 0; i < fieldNum; i++ {
			structField := rt.Field(i)
			tag, hasTag := structField.Tag.Lookup(tagName)
			if !hasTag {
				continue
			}
			columnName := ""
			settingIdx := strings.Index(tag, ",")
			if settingIdx > 0 {
				columnName = tag[0:settingIdx]
			} else {
				columnName = tag
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
				err = fmt.Errorf("get %s failed, %v", cName, decodeErr)
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
		case sql.BoolType:
			v := false
			decodeErr := c.Get(&v)
			if decodeErr != nil {
				err = fmt.Errorf("get %s failed, %v", cName, decodeErr)
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
		case sql.IntType:
			v := int64(0)
			decodeErr := c.Get(&v)
			if decodeErr != nil {
				err = fmt.Errorf("get %s failed, %v", cName, decodeErr)
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
		case sql.FloatType:
			v := 0.0
			decodeErr := c.Get(&v)
			if decodeErr != nil {
				err = fmt.Errorf("get %s failed, %v", cName, decodeErr)
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
		case sql.TimeType:
			v := time.Time{}
			decodeErr := c.Get(&v)
			if decodeErr != nil {
				err = fmt.Errorf("get %s failed, %v", cName, decodeErr)
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
		case sql.BytesType:
			rv.FieldByName(field.Name).SetBytes(c.RawValue())
		case sql.JsonType:
			if field.Type == sqlJsonType || field.Type == sqlSTDJsonType {
				rv.FieldByName(field.Name).Set(reflect.ValueOf(c.RawValue()).Convert(field.Type))
			} else {
				v := reflect.New(field.Type).Interface()
				decodeErr := c.Get(&v)
				if decodeErr != nil {
					err = fmt.Errorf("get %s failed, %v", cName, decodeErr)
					return
				}
				rv.FieldByName(field.Name).Set(reflect.ValueOf(v).Elem())
			}
		case sql.UnknownType:
			if field.Type.AssignableTo(sqlBytesType) {
				rv.FieldByName(field.Name).SetBytes(c.RawValue())
			}
		}
	}
	// load hook
	hookErr := executeLoadMakeupHook(ctx, row)
	if hookErr != nil {
		err = hookErr
	}
	return
}
