package specifications

import (
	stdsql "database/sql"
	"fmt"
	"github.com/aacfactory/errors"
	"github.com/aacfactory/fns/commons/times"
	"github.com/aacfactory/fns/context"
	"github.com/aacfactory/fns/services/authorizations"
	"github.com/aacfactory/json"
	"reflect"
	"time"
)

func BuildInsert[T any](ctx context.Context, entries ...T) (method Method, query []byte, arguments []any, returning []string, err error) {
	dialect, dialectErr := LoadDialect(ctx)
	if dialectErr != nil {
		err = dialectErr
		return
	}
	t := Instance[T]()
	spec, specErr := GetSpecification(ctx, t)
	if specErr != nil {
		err = specErr
		return
	}
	if spec.View {
		err = errors.Warning(fmt.Sprintf("sql: %s is view", spec.Key))
		return
	}
	// audit
	by, at, hasAc := spec.AuditCreation()
	if hasAc {
		for i, entry := range entries {
			auth, hasAuth, loadErr := authorizations.Load(ctx)
			if loadErr != nil {
				err = errors.Warning(fmt.Sprintf("sql: %s need audit deletion", spec.Key)).WithCause(loadErr)
				return
			}
			if !hasAuth {
				err = errors.Warning(fmt.Sprintf("sql: %s need audit deletion", spec.Key)).WithCause(fmt.Errorf("authorization was not found"))
				return
			}
			if !auth.Exist() {
				err = errors.Warning(fmt.Sprintf("sql: %s need audit deletion", spec.Key)).WithCause(authorizations.ErrUnauthorized)
				return
			}
			rv := reflect.ValueOf(&entry)
			if by != nil {
				rby := rv.Elem().FieldByName(by.Field)
				if rby.IsZero() {
					if by.Type.Name == StringType {
						rby.SetString(auth.Id.String())
					} else if by.Type.Name == IntType {
						rby.SetInt(auth.Id.Int())
					}
				}
			}
			if at != nil {
				rat := rv.Elem().FieldByName(at.Field)
				if at.Type.Value.ConvertibleTo(datetimeType) {
					rat.Set(reflect.ValueOf(time.Now()))
				} else if at.Type.Value.ConvertibleTo(nullTimeType) {
					rat.Set(reflect.ValueOf(stdsql.NullTime{
						Time:  time.Now(),
						Valid: true,
					}))
				} else if at.Type.Value.ConvertibleTo(intType) {
					rat.SetInt(time.Now().UnixMilli())
				} else if at.Type.Value.ConvertibleTo(nullInt64Type) {
					rat.Set(reflect.ValueOf(stdsql.NullInt64{
						Int64: time.Now().UnixMilli(),
						Valid: true,
					}))
				}
			}
			entries[i] = entry
		}
	}
	var fields []string
	method, query, fields, returning, err = dialect.Insert(Todo(ctx, t, dialect), spec, len(entries))
	if err != nil {
		return
	}
	for _, entry := range entries {
		args, argsErr := spec.Arguments(entry, fields)
		if argsErr != nil {
			err = argsErr
			return
		}
		arguments = append(arguments, args...)
	}
	return
}

func BuildInsertOrUpdate[T any](ctx context.Context, entry T) (method Method, query []byte, arguments []any, returning []string, err error) {
	dialect, dialectErr := LoadDialect(ctx)
	if dialectErr != nil {
		err = dialectErr
		return
	}
	t := Instance[T]()
	spec, specErr := GetSpecification(ctx, t)
	if specErr != nil {
		err = specErr
		return
	}
	if spec.View {
		err = errors.Warning(fmt.Sprintf("sql: %s is view", spec.Key))
		return
	}
	// audit
	by, at, hasAc := spec.AuditCreation()
	if hasAc {
		auth, hasAuth, loadErr := authorizations.Load(ctx)
		if loadErr != nil {
			err = errors.Warning(fmt.Sprintf("sql: %s need audit deletion", spec.Key)).WithCause(loadErr)
			return
		}
		if !hasAuth {
			err = errors.Warning(fmt.Sprintf("sql: %s need audit deletion", spec.Key)).WithCause(fmt.Errorf("authorization was not found"))
			return
		}
		if !auth.Exist() {
			err = errors.Warning(fmt.Sprintf("sql: %s need audit deletion", spec.Key)).WithCause(authorizations.ErrUnauthorized)
			return
		}
		rv := reflect.ValueOf(&entry)
		if by != nil {
			rby := rv.Elem().FieldByName(by.Field)
			if rby.IsZero() {
				if by.Type.Name == StringType {
					rby.SetString(auth.Id.String())
				} else if by.Type.Name == IntType {
					rby.SetInt(auth.Id.Int())
				}
			}
		}
		if at != nil {
			rat := rv.Elem().FieldByName(at.Field)
			if at.Type.Value.ConvertibleTo(datetimeType) {
				rat.Set(reflect.ValueOf(time.Now()))
			} else if at.Type.Value.ConvertibleTo(nullTimeType) {
				rat.Set(reflect.ValueOf(stdsql.NullTime{
					Time:  time.Now(),
					Valid: true,
				}))
			} else if at.Type.Value.ConvertibleTo(intType) {
				rat.SetInt(time.Now().UnixMilli())
			} else if at.Type.Value.ConvertibleTo(nullInt64Type) {
				rat.Set(reflect.ValueOf(stdsql.NullInt64{
					Int64: time.Now().UnixMilli(),
					Valid: true,
				}))
			}
		}
	}
	var fields []string
	method, query, fields, returning, err = dialect.InsertOrUpdate(Todo(ctx, t, dialect), spec)
	if err != nil {
		return
	}
	arguments, err = spec.Arguments(entry, fields)
	return
}

func BuildInsertWhenExist[T any](ctx context.Context, entry T, src QueryExpr) (method Method, query []byte, arguments []any, returning []string, err error) {
	dialect, dialectErr := LoadDialect(ctx)
	if dialectErr != nil {
		err = dialectErr
		return
	}
	t := Instance[T]()
	spec, specErr := GetSpecification(ctx, t)
	if specErr != nil {
		err = specErr
		return
	}
	if spec.View {
		err = errors.Warning(fmt.Sprintf("sql: %s is view", spec.Key))
		return
	}
	// audit
	by, at, hasAc := spec.AuditCreation()
	if hasAc {
		auth, hasAuth, loadErr := authorizations.Load(ctx)
		if loadErr != nil {
			err = errors.Warning(fmt.Sprintf("sql: %s need audit deletion", spec.Key)).WithCause(loadErr)
			return
		}
		if !hasAuth {
			err = errors.Warning(fmt.Sprintf("sql: %s need audit deletion", spec.Key)).WithCause(fmt.Errorf("authorization was not found"))
			return
		}
		if !auth.Exist() {
			err = errors.Warning(fmt.Sprintf("sql: %s need audit deletion", spec.Key)).WithCause(authorizations.ErrUnauthorized)
			return
		}
		rv := reflect.ValueOf(&entry)
		if by != nil {
			rby := rv.Elem().FieldByName(by.Field)
			if rby.IsZero() {
				if by.Type.Name == StringType {
					rby.SetString(auth.Id.String())
				} else if by.Type.Name == IntType {
					rby.SetInt(auth.Id.Int())
				}
			}
		}
		if at != nil {
			rat := rv.Elem().FieldByName(at.Field)
			if at.Type.Value.ConvertibleTo(datetimeType) {
				rat.Set(reflect.ValueOf(time.Now()))
			} else if at.Type.Value.ConvertibleTo(nullTimeType) {
				rat.Set(reflect.ValueOf(stdsql.NullTime{
					Time:  time.Now(),
					Valid: true,
				}))
			} else if at.Type.Value.ConvertibleTo(intType) {
				rat.SetInt(time.Now().UnixMilli())
			} else if at.Type.Value.ConvertibleTo(nullInt64Type) {
				rat.Set(reflect.ValueOf(stdsql.NullInt64{
					Int64: time.Now().UnixMilli(),
					Valid: true,
				}))
			}
		}
	}
	var fields []string
	var srcArguments []any
	method, query, fields, srcArguments, returning, err = dialect.InsertWhenExist(Todo(ctx, t, dialect), spec, src)
	if err != nil {
		return
	}
	arguments, err = spec.Arguments(entry, fields)
	if err != nil {
		return
	}
	arguments = append(arguments, srcArguments...)
	return
}

func BuildInsertWhenNotExist[T any](ctx context.Context, entry T, src QueryExpr) (method Method, query []byte, arguments []any, returning []string, err error) {
	dialect, dialectErr := LoadDialect(ctx)
	if dialectErr != nil {
		err = dialectErr
		return
	}
	t := Instance[T]()
	spec, specErr := GetSpecification(ctx, t)
	if specErr != nil {
		err = specErr
		return
	}
	if spec.View {
		err = errors.Warning(fmt.Sprintf("sql: %s is view", spec.Key))
		return
	}
	// audit
	by, at, hasAc := spec.AuditCreation()
	if hasAc {
		auth, hasAuth, loadErr := authorizations.Load(ctx)
		if loadErr != nil {
			err = errors.Warning(fmt.Sprintf("sql: %s need audit deletion", spec.Key)).WithCause(loadErr)
			return
		}
		if !hasAuth {
			err = errors.Warning(fmt.Sprintf("sql: %s need audit deletion", spec.Key)).WithCause(fmt.Errorf("authorization was not found"))
			return
		}
		if !auth.Exist() {
			err = errors.Warning(fmt.Sprintf("sql: %s need audit deletion", spec.Key)).WithCause(authorizations.ErrUnauthorized)
			return
		}
		rv := reflect.ValueOf(&entry)
		if by != nil {
			rby := rv.Elem().FieldByName(by.Field)
			if rby.IsZero() {
				if by.Type.Name == StringType {
					rby.SetString(auth.Id.String())
				} else if by.Type.Name == IntType {
					rby.SetInt(auth.Id.Int())
				}
			}
		}
		if at != nil {
			rat := rv.Elem().FieldByName(at.Field)
			if at.Type.Value.ConvertibleTo(datetimeType) {
				rat.Set(reflect.ValueOf(time.Now()))
			} else if at.Type.Value.ConvertibleTo(nullTimeType) {
				rat.Set(reflect.ValueOf(stdsql.NullTime{
					Time:  time.Now(),
					Valid: true,
				}))
			} else if at.Type.Value.ConvertibleTo(intType) {
				rat.SetInt(time.Now().UnixMilli())
			} else if at.Type.Value.ConvertibleTo(nullInt64Type) {
				rat.Set(reflect.ValueOf(stdsql.NullInt64{
					Int64: time.Now().UnixMilli(),
					Valid: true,
				}))
			}
		}
	}
	var fields []string
	var srcArguments []any
	method, query, fields, srcArguments, returning, err = dialect.InsertWhenNotExist(Todo(ctx, t, dialect), spec, src)
	if err != nil {
		return
	}
	arguments, err = spec.Arguments(entry, fields)
	if err != nil {
		return
	}
	arguments = append(arguments, srcArguments...)
	return
}

func BuildUpdate[T any](ctx context.Context, entry T) (method Method, query []byte, arguments []any, err error) {
	dialect, dialectErr := LoadDialect(ctx)
	if dialectErr != nil {
		err = dialectErr
		return
	}
	t := Instance[T]()
	spec, specErr := GetSpecification(ctx, t)
	if specErr != nil {
		err = specErr
		return
	}
	if spec.View {
		err = errors.Warning(fmt.Sprintf("sql: %s is view", spec.Key))
		return
	}
	// audit
	by, at, hasAm := spec.AuditModification()
	if hasAm {
		auth, hasAuth, loadErr := authorizations.Load(ctx)
		if loadErr != nil {
			err = errors.Warning(fmt.Sprintf("sql: %s need audit deletion", spec.Key)).WithCause(loadErr)
			return
		}
		if !hasAuth {
			err = errors.Warning(fmt.Sprintf("sql: %s need audit deletion", spec.Key)).WithCause(fmt.Errorf("authorization was not found"))
			return
		}
		if !auth.Exist() {
			err = errors.Warning(fmt.Sprintf("sql: %s need audit deletion", spec.Key)).WithCause(authorizations.ErrUnauthorized)
			return
		}
		rv := reflect.ValueOf(&entry)
		if by != nil {
			rby := rv.Elem().FieldByName(by.Field)
			if rby.IsZero() {
				if by.Type.Name == StringType {
					rby.SetString(auth.Id.String())
				} else if by.Type.Name == IntType {
					rby.SetInt(auth.Id.Int())
				}
			}
		}
		if at != nil {
			rat := rv.Elem().FieldByName(at.Field)
			if at.Type.Value.ConvertibleTo(datetimeType) {
				rat.Set(reflect.ValueOf(time.Now()))
			} else if at.Type.Value.ConvertibleTo(nullTimeType) {
				rat.Set(reflect.ValueOf(stdsql.NullTime{
					Time:  time.Now(),
					Valid: true,
				}))
			} else if at.Type.Value.ConvertibleTo(intType) {
				rat.SetInt(time.Now().UnixMilli())
			} else if at.Type.Value.ConvertibleTo(nullInt64Type) {
				rat.Set(reflect.ValueOf(stdsql.NullInt64{
					Int64: time.Now().UnixMilli(),
					Valid: true,
				}))
			}
		}
	}
	var fields []string
	method, query, fields, err = dialect.Update(Todo(ctx, t, dialect), spec)
	if err != nil {
		return
	}
	arguments, err = spec.Arguments(entry, fields)
	return
}

func BuildUpdateFields[T any](ctx context.Context, fields []FieldValue, cond Condition) (method Method, query []byte, arguments []any, err error) {
	dialect, dialectErr := LoadDialect(ctx)
	if dialectErr != nil {
		err = dialectErr
		return
	}
	t := Instance[T]()
	spec, specErr := GetSpecification(ctx, t)
	if specErr != nil {
		err = specErr
		return
	}
	if spec.View {
		err = errors.Warning(fmt.Sprintf("sql: %s is view", spec.Key))
		return
	}
	// audit
	by, at, hasAm := spec.AuditModification()
	if hasAm {
		auth, hasAuth, loadErr := authorizations.Load(ctx)
		if loadErr != nil {
			err = errors.Warning(fmt.Sprintf("sql: %s need audit deletion", spec.Key)).WithCause(loadErr)
			return
		}
		if !hasAuth {
			err = errors.Warning(fmt.Sprintf("sql: %s need audit deletion", spec.Key)).WithCause(fmt.Errorf("authorization was not found"))
			return
		}
		if !auth.Exist() {
			err = errors.Warning(fmt.Sprintf("sql: %s need audit deletion", spec.Key)).WithCause(authorizations.ErrUnauthorized)
			return
		}
		if by != nil {
			exist := false
			for _, field := range fields {
				if field.Name == by.Field {
					exist = true
					break
				}
			}
			if !exist {
				if by.Type.Name == StringType {
					fields = append(fields, FieldValue{
						Name:  by.Field,
						Value: auth.Id.String(),
					})
				} else if by.Type.Name == IntType {
					fields = append(fields, FieldValue{
						Name:  by.Field,
						Value: auth.Id.Int(),
					})
				}
			}
		}
		if at != nil {
			exist := false
			for _, field := range fields {
				if field.Name == at.Field {
					exist = true
					break
				}
			}
			if !exist {
				if at.Type.Value.ConvertibleTo(datetimeType) {
					fields = append(fields, FieldValue{
						Name:  at.Field,
						Value: time.Now(),
					})
				} else if at.Type.Value.ConvertibleTo(nullTimeType) {
					fields = append(fields, FieldValue{
						Name: at.Field,
						Value: stdsql.NullTime{
							Time:  time.Now(),
							Valid: true,
						},
					})
				} else if at.Type.Value.ConvertibleTo(intType) {
					fields = append(fields, FieldValue{
						Name:  at.Field,
						Value: time.Now().UnixMilli(),
					})
				} else if at.Type.Value.ConvertibleTo(nullInt64Type) {
					fields = append(fields, FieldValue{
						Name: at.Field,
						Value: stdsql.NullInt64{
							Int64: time.Now().UnixMilli(),
							Valid: true,
						},
					})
				}
			}
		}
	}
	for i, field := range fields {
		column, hasColumn := spec.ColumnByField(field.Name)
		if !hasColumn {
			err = errors.Warning(fmt.Sprintf("sql: %s field was not found", field.Name)).WithMeta("table", spec.Key)
			return
		}
		switch column.Type.Name {
		case DateType:
			fv, ok := field.Value.(times.Date)
			if !ok {
				err = errors.Warning(fmt.Sprintf("sql: %s field value type must be times.Date", field.Name)).WithMeta("table", spec.Key)
				return
			}
			field.Value = fv.ToTime()
			fields[i] = field
			break
		case TimeType:
			fv, ok := field.Value.(times.Time)
			if !ok {
				err = errors.Warning(fmt.Sprintf("sql: %s field value type must be times.Time", field.Name)).WithMeta("table", spec.Key)
				return
			}
			field.Value = fv.ToTime()
			fields[i] = field
			break
		case JsonType:
			p, encodeErr := json.Marshal(field.Value)
			if encodeErr != nil {
				err = errors.Warning(fmt.Sprintf("sql: encode %s field value failed", field.Name)).WithMeta("table", spec.Key)
				return
			}
			field.Value = p
			fields[i] = field
			break
		case MappingType:
			if column.Kind != Reference {
				err = errors.Warning(fmt.Sprintf("sql: kind %s field value type can not be updated", field.Name)).WithMeta("table", spec.Key)
				return
			}
			rv := reflect.ValueOf(&t)
			rv.FieldByName(column.Field).Set(reflect.ValueOf(field.Value))
			arguments, err = spec.Arguments(t, []string{column.Field})
			if err != nil {
				err = errors.Warning(fmt.Sprintf("sql: scan reference %s field value faield", field.Name)).WithCause(err).WithMeta("table", spec.Key)
				return
			}
			field.Value = arguments[0]
			fields[i] = field
			break
		}
	}
	method, query, arguments, err = dialect.UpdateFields(Todo(ctx, t, dialect), spec, fields, cond)
	if err != nil {
		return
	}
	return
}

func BuildDelete[T any](ctx context.Context, entry T) (method Method, query []byte, arguments []any, err error) {
	dialect, dialectErr := LoadDialect(ctx)
	if dialectErr != nil {
		err = dialectErr
		return
	}
	t := Instance[T]()
	spec, specErr := GetSpecification(ctx, t)
	if specErr != nil {
		err = specErr
		return
	}
	if spec.View {
		err = errors.Warning(fmt.Sprintf("sql: %s is view", spec.Key))
		return
	}
	var fields []string
	method, query, fields, err = dialect.Delete(Todo(ctx, t, dialect), spec)
	if err != nil {
		return
	}
	by, at, hasAd := spec.AuditDeletion()
	if hasAd {
		auth, hasAuth, loadErr := authorizations.Load(ctx)
		if loadErr != nil {
			err = errors.Warning(fmt.Sprintf("sql: %s need audit deletion", spec.Key)).WithCause(loadErr)
			return
		}
		if !hasAuth {
			err = errors.Warning(fmt.Sprintf("sql: %s need audit deletion", spec.Key)).WithCause(fmt.Errorf("authorization was not found"))
			return
		}
		if !auth.Exist() {
			err = errors.Warning(fmt.Sprintf("sql: %s need audit deletion", spec.Key)).WithCause(authorizations.ErrUnauthorized)
			return
		}
		rv := reflect.ValueOf(&entry)
		if by != nil {
			rby := rv.Elem().FieldByName(by.Field)
			if rby.IsZero() {
				if by.Type.Name == StringType {
					rby.SetString(auth.Id.String())
				} else if by.Type.Name == IntType {
					rby.SetInt(auth.Id.Int())
				}
			}
		}
		if at != nil {
			rat := rv.Elem().FieldByName(at.Field)
			if at.Type.Value.ConvertibleTo(datetimeType) {
				rat.Set(reflect.ValueOf(time.Now()))
			} else if at.Type.Value.ConvertibleTo(nullTimeType) {
				rat.Set(reflect.ValueOf(stdsql.NullTime{
					Time:  time.Now(),
					Valid: true,
				}))
			} else if at.Type.Value.ConvertibleTo(intType) {
				rat.SetInt(time.Now().UnixMilli())
			} else if at.Type.Value.ConvertibleTo(nullInt64Type) {
				rat.Set(reflect.ValueOf(stdsql.NullInt64{
					Int64: time.Now().UnixMilli(),
					Valid: true,
				}))
			}
		}
	}
	arguments, err = spec.Arguments(entry, fields)
	return
}

func BuildDeleteAnyByCondition(ctx context.Context, entry any, cond Condition) (method Method, query []byte, arguments []any, err error) {
	dialect, dialectErr := LoadDialect(ctx)
	if dialectErr != nil {
		err = dialectErr
		return
	}
	spec, specErr := GetSpecification(ctx, entry)
	if specErr != nil {
		err = specErr
		return
	}
	if spec.View {
		err = errors.Warning(fmt.Sprintf("sql: %s is view", spec.Key))
		return
	}
	var audits []string
	method, query, audits, arguments, err = dialect.DeleteByConditions(Todo(ctx, entry, dialect), spec, cond)
	if err != nil {
		return
	}
	if len(audits) > 0 {
		by, at, hasAd := spec.AuditDeletion()
		if !hasAd {
			err = errors.Warning(fmt.Sprintf("sql: %s need audit deletion", spec.Key)).WithCause(fmt.Errorf("dialect return audits but entry has no audit deletion"))
			return
		}
		auth, hasAuth, loadErr := authorizations.Load(ctx)
		if loadErr != nil {
			err = errors.Warning(fmt.Sprintf("sql: %s need audit deletion", spec.Key)).WithCause(loadErr)
			return
		}
		if !hasAuth {
			err = errors.Warning(fmt.Sprintf("sql: %s need audit deletion", spec.Key)).WithCause(fmt.Errorf("authorization was not found"))
			return
		}
		if !auth.Exist() {
			err = errors.Warning(fmt.Sprintf("sql: %s need audit deletion", spec.Key)).WithCause(authorizations.ErrUnauthorized)
			return
		}
		auditArgs := make([]any, 0, 2)
		for _, auditFieldName := range audits {
			column, hasColumn := spec.ColumnByField(auditFieldName)
			if !hasColumn {
				err = errors.Warning(fmt.Sprintf("sql: %s need audit deletion", spec.Key)).WithCause(fmt.Errorf("column was not found")).WithMeta("field", auditFieldName)
				return
			}
			if by != nil && column.Name == by.Name {
				if by.Type.Name == StringType {
					auditArgs = append(auditArgs, auth.Id.String())
				} else if by.Type.Name == IntType {
					auditArgs = append(auditArgs, auth.Id.Int())
				}
			} else if at != nil && column.Name == at.Name {
				if at.Type.Value.ConvertibleTo(datetimeType) {
					auditArgs = append(auditArgs, time.Now())
				} else if at.Type.Value.ConvertibleTo(nullTimeType) {
					auditArgs = append(auditArgs, stdsql.NullTime{
						Time:  time.Now(),
						Valid: true,
					})
				} else if at.Type.Value.ConvertibleTo(intType) {
					auditArgs = append(auditArgs, time.Now().UnixMilli())
				} else if at.Type.Value.ConvertibleTo(nullInt64Type) {
					auditArgs = append(auditArgs, stdsql.NullInt64{
						Int64: time.Now().UnixMilli(),
						Valid: true,
					})
				}
			}
		}
		arguments = append(auditArgs, arguments...)
	}
	return
}

func BuildDeleteByCondition[T any](ctx context.Context, cond Condition) (method Method, query []byte, arguments []any, err error) {
	method, query, arguments, err = BuildDeleteAnyByCondition(ctx, Instance[T](), cond)
	if err != nil {
		return
	}
	return
}

func BuildCount[T any](ctx context.Context, cond Condition) (method Method, query []byte, arguments []any, err error) {
	dialect, dialectErr := LoadDialect(ctx)
	if dialectErr != nil {
		err = dialectErr
		return
	}
	t := Instance[T]()
	spec, specErr := GetSpecification(ctx, t)
	if specErr != nil {
		err = specErr
		return
	}
	method, query, arguments, err = dialect.Count(Todo(ctx, t, dialect), spec, cond)
	if err != nil {
		return
	}
	return
}

func BuildExist[T any](ctx context.Context, cond Condition) (method Method, query []byte, arguments []any, err error) {
	dialect, dialectErr := LoadDialect(ctx)
	if dialectErr != nil {
		err = dialectErr
		return
	}
	t := Instance[T]()
	spec, specErr := GetSpecification(ctx, t)
	if specErr != nil {
		err = specErr
		return
	}
	method, query, arguments, err = dialect.Exist(Todo(ctx, t, dialect), spec, cond)
	if err != nil {
		return
	}
	return
}

func BuildQuery[T any](ctx context.Context, cond Condition, orders Orders, groupBy GroupBy, having Having, offset int, length int) (method Method, query []byte, arguments []any, columns []string, err error) {
	dialect, dialectErr := LoadDialect(ctx)
	if dialectErr != nil {
		err = dialectErr
		return
	}
	t := Instance[T]()
	spec, specErr := GetSpecification(ctx, t)
	if specErr != nil {
		err = specErr
		return
	}
	method, query, arguments, columns, err = dialect.Query(Todo(ctx, t, dialect), spec, cond, orders, groupBy, having, offset, length)
	if err != nil {
		return
	}
	return
}
