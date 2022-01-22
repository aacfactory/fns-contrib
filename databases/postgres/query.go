package postgres

import (
	db "database/sql"
	stdJson "encoding/json"
	"fmt"
	"github.com/aacfactory/errors"
	"github.com/aacfactory/fns"
	"github.com/aacfactory/fns-contrib/databases/sql"
	"github.com/aacfactory/json"
	"reflect"
	"strings"
	"time"
)

func Query(ctx fns.Context, cond *Conditions, rows interface{}) (fetched bool, err errors.CodeError) {
	fetched0, queryErr := query0(ctx, cond, nil, nil, rows)
	if queryErr != nil {
		err = errors.ServiceError("fns Postgres: query failed").WithCause(queryErr).WithMeta("_fns_postgres", "Query")
		return
	}
	fetched = fetched0
	return
}

func QueryWithRange(ctx fns.Context, cond *Conditions, orders *Orders, rng *Range, rows interface{}) (fetched bool, err errors.CodeError) {
	fetched0, queryErr := query0(ctx, cond, orders, rng, rows)
	if queryErr != nil {
		err = errors.ServiceError("fns Postgres: query with range failed").WithCause(queryErr).WithMeta("_fns_postgres", "QueryWithRange")
		return
	}
	fetched = fetched0
	return
}

func QueryOne(ctx fns.Context, cond *Conditions, row interface{}) (fetched bool, err errors.CodeError) {
	if row == nil {
		err = errors.ServiceError("fns Postgres: query one failed for row is nil").WithMeta("_fns_postgres", "QueryOne")
		return
	}
	rv := reflect.ValueOf(row)
	if rv.Type().Kind() != reflect.Ptr {
		err = errors.ServiceError("fns Postgres: query one failed for type of row is not ptr").WithMeta("_fns_postgres", "QueryOne")
		return
	}
	if rv.Elem().Type().Kind() != reflect.Struct {
		err = errors.ServiceError("fns Postgres: query one failed for type of row is not ptr struct").WithMeta("_fns_postgres", "QueryOne")
		return
	}
	rowsRV := reflect.New(reflect.SliceOf(reflect.TypeOf(row)))
	rows := rowsRV.Interface()
	fetched0, queryErr := query0(ctx, cond, nil, nil, rows)
	if queryErr != nil {
		err = errors.ServiceError("fns Postgres: query one failed").WithCause(queryErr).WithMeta("_fns_postgres", "QueryOne")
		return
	}
	if !fetched0 {
		return
	}
	fetched = fetched0
	rv.Elem().Set(rowsRV.Elem().Index(0).Elem())
	return
}

func QueryDirect(ctx fns.Context, query string, args *sql.Tuple, rows interface{}) (fetched bool, err error) {
	// query
	results, queryErr := sql.Query(ctx, sql.Param{
		Query: query,
		Args:  args,
	})
	if queryErr != nil {
		err = errors.ServiceError("fns Postgres: query direct failed").WithCause(queryErr).WithMeta("_fns_postgres", "QueryDirect")
		return
	}
	fetched = !results.Empty()
	if !fetched {
		return
	}
	rv := reflect.ValueOf(rows)
	scanErr := scanQueryResults(results, rv)
	if scanErr != nil {
		err = errors.ServiceError("fns Postgres: query direct failed").WithCause(scanErr).WithMeta("_fns_postgres", "QueryDirect")
		return
	}
	return
}

func query0(ctx fns.Context, cond *Conditions, orders *Orders, rng *Range, rows interface{}) (fetched bool, err error) {
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
	results, queryErr := sql.Query(ctx, sql.Param{
		Query: query,
		Args:  args,
	})
	if queryErr != nil {
		err = queryErr
		return
	}
	fetched = !results.Empty()
	if !fetched {
		return
	}

	scanErr := scanQueryResults(results, rv)
	if scanErr != nil {
		err = scanErr
		return
	}
	return
}

func scanQueryResults(results *sql.Rows, rows reflect.Value) (err error) {
	rv := rows.Elem()
	for {
		result, has := results.Next()
		if !has {
			break
		}
		row := reflect.New(rv.Type().Elem().Elem())
		err = scanQueryResult(result, row)
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

func scanQueryResult(result *sql.Row, row reflect.Value) (err error) {
	rv := row.Elem()
	rt := rv.Type()
	fieldNum := rt.NumField()
	columns := result.Columns()
	for _, c := range columns {
		if c.Nil {
			continue
		}
		cName := strings.ToUpper(strings.TrimSpace(c.Name))
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
		switch c.Type {
		case sql.StringType:
			v := ""
			decodeErr := c.Decode(&v)
			if decodeErr != nil {
				err = fmt.Errorf("decode %s failed, %v", cName, decodeErr)
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
			decodeErr := c.Decode(&v)
			if decodeErr != nil {
				err = fmt.Errorf("decode %s failed, %v", cName, decodeErr)
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
			decodeErr := c.Decode(&v)
			if decodeErr != nil {
				err = fmt.Errorf("decode %s failed, %v", cName, decodeErr)
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
			decodeErr := c.Decode(&v)
			if decodeErr != nil {
				err = fmt.Errorf("decode %s failed, %v", cName, decodeErr)
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
			decodeErr := c.Decode(&v)
			if decodeErr != nil {
				err = fmt.Errorf("decode %s failed, %v", cName, decodeErr)
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
			rv.FieldByName(field.Name).SetBytes(c.Value)
		case sql.JsonType:
			if field.Type == sqlJsonType || field.Type == sqlSTDJsonType {
				rv.FieldByName(field.Name).Set(reflect.ValueOf(c.Value).Convert(field.Type))
			} else {
				v := reflect.New(field.Type).Interface()
				decodeErr := c.Decode(&v)
				if decodeErr != nil {
					err = fmt.Errorf("decode %s failed, %v", cName, decodeErr)
					return
				}
				rv.FieldByName(field.Name).Set(reflect.ValueOf(v).Elem())
			}
		case sql.UnknownType:
			if field.Type.AssignableTo(sqlBytesType) {
				rv.FieldByName(field.Name).SetBytes(c.Value)
			}
		}
	}
	return
}
