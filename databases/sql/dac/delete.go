package dac

import (
	"github.com/aacfactory/errors"
	"github.com/aacfactory/fns-contrib/databases/sql"
	"github.com/aacfactory/fns-contrib/databases/sql/dac/conditions"
	"github.com/aacfactory/fns-contrib/databases/sql/dac/specifications"
	"github.com/aacfactory/fns/context"
	"reflect"
)

func Delete[T Table](ctx context.Context, entry T) (v T, affected int64, err error) {
	spec, specErr := specifications.GetSpecification(ctx, entry)
	if specErr != nil {
		err = errors.Warning("sql: delete failed").WithCause(specErr)
		return
	}
	cascadesColumns, hasCascades := spec.DeleteCascadeColumns()
	if hasCascades {
		begins := false
		rv := reflect.ValueOf(&entry).Elem()
		for _, column := range cascadesColumns {
			host, target, _, mapping, _, _, has := column.Links()
			if has {
				hv := rv.FieldByName(host).Interface()
				mv := reflect.New(mapping.Type).Elem().Interface().(Table)
				_, query, arguments, buildErr := specifications.BuildDeleteAnyByCondition(ctx, mv, specifications.Condition{Condition: Eq(target, hv)})
				if buildErr != nil {
					sql.Rollback(ctx)
					err = errors.Warning("sql: delete failed").WithCause(buildErr)
					return
				}
				if !begins {
					beginErr := sql.Begin(ctx)
					if beginErr != nil {
						err = errors.Warning("sql: delete failed").WithCause(beginErr)
						return
					}
					begins = true
				}
				_, execErr := sql.Execute(ctx, query, arguments...)
				if execErr != nil {
					sql.Rollback(ctx)
					err = errors.Warning("sql: delete failed").WithCause(execErr)
					return
				}
			}
			// link
			host, target, _, mapping, has = column.Link()
			if has {
				hv := rv.FieldByName(host).Interface()
				mv := reflect.New(mapping.Type).Elem().Interface().(Table)
				_, query, arguments, buildErr := specifications.BuildDeleteAnyByCondition(ctx, mv, specifications.Condition{Condition: Eq(target, hv)})
				if buildErr != nil {
					sql.Rollback(ctx)
					err = errors.Warning("sql: delete failed").WithCause(buildErr)
					return
				}
				if !begins {
					beginErr := sql.Begin(ctx)
					if beginErr != nil {
						err = errors.Warning("sql: delete failed").WithCause(beginErr)
						return
					}
					begins = true
				}
				_, execErr := sql.Execute(ctx, query, arguments...)
				if execErr != nil {
					sql.Rollback(ctx)
					err = errors.Warning("sql: delete failed").WithCause(execErr)
					return
				}
			}
		}
	}

	_, query, arguments, buildErr := specifications.BuildDelete[T](ctx, entry)
	if buildErr != nil {
		if hasCascades {
			sql.Rollback(ctx)
		}
		err = errors.Warning("sql: delete failed").WithCause(buildErr)
		return
	}
	result, execErr := sql.Execute(ctx, query, arguments...)
	if execErr != nil {
		if hasCascades {
			sql.Rollback(ctx)
		}
		err = errors.Warning("sql: delete failed").WithCause(execErr)
		return
	}
	if hasCascades {
		cmtErr := sql.Commit(ctx)
		if cmtErr != nil {
			err = errors.Warning("sql: delete failed").WithCause(cmtErr)
			return
		}
	}
	if affected = result.RowsAffected; affected == 1 {
		v = entry
	}
	return
}

func DeleteByCondition[T Table](ctx context.Context, cond conditions.Condition) (affected int64, err error) {
	spec, specErr := specifications.GetSpecification(ctx, specifications.Instance[T]())
	if specErr != nil {
		err = errors.Warning("sql: delete failed").WithCause(specErr)
		return
	}
	cascadesColumns, hasCascades := spec.DeleteCascadeColumns()
	if hasCascades {
		entries, queryErr := ALL[T](ctx, Conditions(cond))
		if queryErr != nil {
			err = errors.Warning("sql: delete by condition failed").WithCause(queryErr)
			return
		}
		if len(entries) == 0 {
			return
		}
		begins := false
		for _, entry := range entries {
			rv := reflect.ValueOf(&entry).Elem()
			for _, column := range cascadesColumns {
				host, target, _, mapping, _, _, has := column.Links()
				if has {
					hv := rv.FieldByName(host).Interface()
					mv := reflect.New(mapping.Type).Elem().Interface().(Table)
					_, query, arguments, buildErr := specifications.BuildDeleteAnyByCondition(ctx, mv, specifications.Condition{Condition: Eq(target, hv)})
					if buildErr != nil {
						sql.Rollback(ctx)
						err = errors.Warning("sql: delete failed").WithCause(buildErr)
						return
					}
					if !begins {
						beginErr := sql.Begin(ctx)
						if beginErr != nil {
							err = errors.Warning("sql: delete failed").WithCause(beginErr)
							return
						}
						begins = true
					}
					_, execErr := sql.Execute(ctx, query, arguments...)
					if execErr != nil {
						sql.Rollback(ctx)
						err = errors.Warning("sql: delete failed").WithCause(execErr)
						return
					}
				}
				// link
				host, target, _, mapping, has = column.Link()
				if has {
					hv := rv.FieldByName(host).Interface()
					mv := reflect.New(mapping.Type).Elem().Interface().(Table)
					_, query, arguments, buildErr := specifications.BuildDeleteAnyByCondition(ctx, mv, specifications.Condition{Condition: Eq(target, hv)})
					if buildErr != nil {
						sql.Rollback(ctx)
						err = errors.Warning("sql: delete failed").WithCause(buildErr)
						return
					}
					if !begins {
						beginErr := sql.Begin(ctx)
						if beginErr != nil {
							err = errors.Warning("sql: delete failed").WithCause(beginErr)
							return
						}
						begins = true
					}
					_, execErr := sql.Execute(ctx, query, arguments...)
					if execErr != nil {
						sql.Rollback(ctx)
						err = errors.Warning("sql: delete failed").WithCause(execErr)
						return
					}
				}
			}
		}
	}

	_, query, arguments, buildErr := specifications.BuildDeleteByCondition[T](ctx, specifications.Condition{Condition: cond})
	if buildErr != nil {
		if hasCascades {
			sql.Rollback(ctx)
		}
		err = errors.Warning("sql: delete by condition failed").WithCause(buildErr)
		return
	}
	result, execErr := sql.Execute(ctx, query, arguments...)
	if execErr != nil {
		if hasCascades {
			sql.Rollback(ctx)
		}
		err = errors.Warning("sql: delete by condition failed").WithCause(execErr)
		return
	}
	if hasCascades {
		cmtErr := sql.Commit(ctx)
		if cmtErr != nil {
			err = errors.Warning("sql: delete by condition failed").WithCause(cmtErr)
			return
		}
	}
	affected = result.RowsAffected
	return
}
