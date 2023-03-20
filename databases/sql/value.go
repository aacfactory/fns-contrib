package sql

import (
	stdsql "database/sql"
	"github.com/aacfactory/errors"
	"reflect"
	"strings"
)

type ValueType interface {
	Type() (typ reflect.Type)
	ColumnType() (ct string)
	DatabaseTypes() (types []string)
	Scanner() (scanner ValueScanner)
	Encode(src any) (p []byte, err error)
	Decode(p []byte) (v any, err error)
}

type ValueScanner interface {
	stdsql.Scanner
	Value() (value any)
	Reset()
}

func RegisterType(vt ValueType) {
	if vt == nil {
		panic(errors.Warning("sql: register value type failed").WithCause(errors.Warning("sql: target is nil")))
		return
	}
	typ := vt.Type()
	if typ == nil {
		panic(errors.Warning("sql: register value type failed").WithCause(errors.Warning("sql: reflect type is nil")))
		return
	}
	ct := vt.ColumnType()
	if ct == "" {
		panic(errors.Warning("sql: register value type failed").WithCause(errors.Warning("sql: column type is required")))
		return
	}
	dbTypes := vt.DatabaseTypes()
	if dbTypes == nil || len(dbTypes) == 0 {
		panic(errors.Warning("sql: register value type failed").WithCause(errors.Warning("sql: database types is required")))
		return
	}
	// vts
	key := typ.String()
	_, exist := valueTypes[key]
	if exist {
		panic(errors.Warning("sql: register value type failed").WithCause(errors.Warning("sql: target was registered").WithMeta("type", key)))
		return
	}
	valueTypes[key] = vt
	// ct
	_, exist = columnTypes[ct]
	if exist {
		panic(errors.Warning("sql: register value type failed").WithCause(errors.Warning("sql: column type of target was registered").WithMeta("type", key).WithMeta("columnType", string(ct))))
		return
	}
	columnTypes[ct] = vt
	// dbt
	for _, dbType := range dbTypes {
		dbType = strings.ToUpper(strings.TrimSpace(dbType))
		if dbType == "" {
			panic(errors.Warning("sql: register value type failed").WithCause(errors.Warning("sql: one database type of target was nil").WithMeta("type", key)))
			return
		}
		_, exist = databaseTypes[dbType]
		if exist {
			panic(errors.Warning("sql: register value type failed").WithCause(errors.Warning("sql: database type of target was registered").WithMeta("type", key).WithMeta("databaseType", dbType)))
			return
		}
		databaseTypes[dbType] = vt
	}

	return
}

var (
	valueTypes    = make(map[string]ValueType)
	columnTypes   = make(map[string]ValueType)
	databaseTypes = make(map[string]ValueType)
)

func findValueTypeByColumnType(ct string) (vt ValueType, has bool) {
	if ct != "" {
		vt, has = columnTypes[ct]
		if has {
			return
		}
	}
	return
}

func findValueTypeByDatabaseType(dbt string) (vt ValueType, has bool) {
	if dbt != "" {
		dbt = strings.ToUpper(dbt)
		vt, has = databaseTypes[dbt]
		if has {
			return
		}
	}
	return
}

func init() {
	RegisterType(StringValueType())
	RegisterType(BoolValueType())
	RegisterType(IntValueType())
	RegisterType(FloatValueType())
	RegisterType(DatetimeValueType())
	RegisterType(DateValueType())
	RegisterType(TimeValueType())
	RegisterType(BytesValueType())
	RegisterType(JsonValueType())
}
