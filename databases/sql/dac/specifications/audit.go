package specifications

import (
	stdsql "database/sql"
	"fmt"
	"github.com/aacfactory/errors"
	ssql "github.com/aacfactory/fns-contrib/databases/sql"
	"github.com/aacfactory/fns/commons/uid"
	"github.com/aacfactory/fns/context"
	"github.com/aacfactory/fns/services/authorizations"
	"reflect"
	"time"
)

func setupAudit[T any](by *Column, at *Column, auth authorizations.Authorization, entries []T) {
	for i, entry := range entries {
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
			} else if at.Type.Value.ConvertibleTo(nullDatetimeType) {
				rat.Set(reflect.ValueOf(ssql.NullDatetime{
					NullTime: stdsql.NullTime{
						Time:  time.Now(),
						Valid: true,
					},
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
	return
}

func TrySetupAuditCreation[T any](ctx context.Context, spec *Specification, entries []T) (err error) {
	// id
	pk, hasPk := spec.Pk()
	// creation
	by, at, has := spec.AuditCreation()
	if !has {
		if hasPk && !pk.Incr() {
			for i, entry := range entries {
				rv := reflect.ValueOf(&entry)
				pkf := rv.Elem().FieldByName(pk.Field)
				if pkf.IsZero() {
					pkf.SetString(uid.UID())
					entries[i] = entry
				}
			}
		}
		return
	}
	auth, hasAuth, loadErr := authorizations.Load(ctx)
	if loadErr != nil {
		err = errors.Warning(fmt.Sprintf("sql: %s need audit creation", spec.Key)).WithCause(loadErr)
		return
	}
	if !hasAuth {
		err = errors.Warning(fmt.Sprintf("sql: %s need audit creation", spec.Key)).WithCause(fmt.Errorf("authorization was not found"))
		return
	}
	if !auth.Exist() {
		err = errors.Warning(fmt.Sprintf("sql: %s need audit creation", spec.Key)).WithCause(authorizations.ErrUnauthorized)
		return
	}
	for i, entry := range entries {
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
			} else if at.Type.Value.ConvertibleTo(nullDatetimeType) {
				rat.Set(reflect.ValueOf(ssql.NullDatetime{
					NullTime: stdsql.NullTime{
						Time:  time.Now(),
						Valid: true,
					},
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
		if hasPk && !pk.Incr() {
			pkf := rv.Elem().FieldByName(pk.Field)
			if pkf.IsZero() {
				pkf.SetString(uid.UID())
			}
		}
		entries[i] = entry
	}
	return
}

func TrySetupAuditModification[T any](ctx context.Context, spec *Specification, entries []T) (err error) {
	by, at, has := spec.AuditModification()
	if !has {
		return
	}
	auth, hasAuth, loadErr := authorizations.Load(ctx)
	if loadErr != nil {
		err = errors.Warning(fmt.Sprintf("sql: %s need audit modification", spec.Key)).WithCause(loadErr)
		return
	}
	if !hasAuth {
		err = errors.Warning(fmt.Sprintf("sql: %s need audit modification", spec.Key)).WithCause(fmt.Errorf("authorization was not found"))
		return
	}
	if !auth.Exist() {
		err = errors.Warning(fmt.Sprintf("sql: %s need audit modification", spec.Key)).WithCause(authorizations.ErrUnauthorized)
		return
	}
	setupAudit[T](by, at, auth, entries)
	return
}

func TrySetupAuditDeletion[T any](ctx context.Context, spec *Specification, entries []T) (err error) {
	by, at, has := spec.AuditDeletion()
	if !has {
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
	setupAudit[T](by, at, auth, entries)
	return
}

func TrySetupAuditVersion[T any](ctx context.Context, entries []T) (err error) {
	spec, specErr := GetSpecification(ctx, entries[0])
	if specErr != nil {
		err = specErr
		return
	}
	column, has := spec.AuditVersion()
	if !has {
		return
	}
	for i, entry := range entries {
		rv := reflect.ValueOf(&entry)
		rvt := rv.Elem().FieldByName(column.Field)
		pv := rvt.Int()
		rvt.SetInt(pv + 1)
		entries[i] = entry
	}
	return
}

func TrySetupLastInsertId[T any](ctx context.Context, entry T, id int64) (v T, err error) {
	spec, specErr := GetSpecification(ctx, entry)
	if specErr != nil {
		err = specErr
		return
	}
	column, has := spec.Pk()
	if !has {
		err = errors.Warning(fmt.Sprintf("sql: %s has no pk", spec.Key))
		return
	}
	if !column.Incr() {
		err = errors.Warning(fmt.Sprintf("sql: %s has no incr pk", spec.Key))
		return
	}
	rv := reflect.ValueOf(&entry)
	rvt := rv.Elem().FieldByName(column.Field)
	rvt.SetInt(id)
	v = rv.Elem().Interface().(T)
	return
}
