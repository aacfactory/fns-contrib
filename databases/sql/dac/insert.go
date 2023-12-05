package dac

import (
	"github.com/aacfactory/errors"
	"github.com/aacfactory/fns-contrib/databases/sql"
	"github.com/aacfactory/fns-contrib/databases/sql/dac/conditions"
	"github.com/aacfactory/fns-contrib/databases/sql/dac/specifications"
	"github.com/aacfactory/fns/context"
)

func Insert[T Table](ctx context.Context, entry T) (v T, ok bool, err error) {
	entries := []T{entry}
	method, query, arguments, returning, buildErr := specifications.BuildInsert[T](ctx, entries)
	if buildErr != nil {
		err = errors.Warning("sql: insert failed").WithCause(buildErr)
		return
	}
	if method == specifications.QueryMethod {
		rows, queryErr := sql.Query(ctx, query, arguments...)
		if queryErr != nil {
			err = errors.Warning("sql: insert failed").WithCause(queryErr)
			return
		}
		affected, wErr := specifications.WriteInsertReturning[T](ctx, rows, returning, entries)
		_ = rows.Close()
		if wErr != nil {
			err = errors.Warning("sql: insert failed").WithCause(wErr)
			return
		}
		ok = affected > 0
		if ok {
			verErr := specifications.TrySetupAuditVersion[T](ctx, entries)
			if verErr != nil {
				err = errors.Warning("sql: insert failed").WithCause(verErr)
				return
			}
			v = entries[0]
		}
	} else {
		result, execErr := sql.Execute(ctx, query, arguments...)
		if execErr != nil {
			err = errors.Warning("sql: insert failed").WithCause(execErr)
			return
		}
		ok = result.RowsAffected > 0
		if ok {
			verErr := specifications.TrySetupAuditVersion[T](ctx, entries)
			if verErr != nil {
				err = errors.Warning("sql: insert failed").WithCause(verErr)
				return
			}
			v = entries[0]
		}
	}
	return
}

func InsertMulti[T Table](ctx context.Context, entries []T) (v []T, affected int64, err error) {
	if len(entries) == 0 {
		return
	}
	method, query, arguments, returning, buildErr := specifications.BuildInsert[T](ctx, entries)
	if buildErr != nil {
		err = errors.Warning("sql: insert multi failed").WithCause(buildErr)
		return
	}
	if method == specifications.QueryMethod {
		rows, queryErr := sql.Query(ctx, query, arguments...)
		if queryErr != nil {
			err = errors.Warning("sql: insert multi failed").WithCause(queryErr)
			return
		}
		affected, err = specifications.WriteInsertReturning[T](ctx, rows, returning, entries)
		_ = rows.Close()
		if err != nil {
			err = errors.Warning("sql: insert multi failed").WithCause(err)
			return
		}
		if affected > 0 {
			verErr := specifications.TrySetupAuditVersion[T](ctx, entries)
			if verErr != nil {
				err = errors.Warning("sql: insert multi failed").WithCause(verErr)
				return
			}
			v = entries
		}
	} else {
		result, execErr := sql.Execute(ctx, query, arguments...)
		if execErr != nil {
			err = errors.Warning("sql: insert multi failed").WithCause(execErr)
			return
		}
		affected = result.RowsAffected
		if affected > 0 {
			verErr := specifications.TrySetupAuditVersion[T](ctx, entries)
			if verErr != nil {
				err = errors.Warning("sql: insert multi failed").WithCause(verErr)
				return
			}
			v = entries
		}
	}
	return
}

func InsertOrUpdate[T Table](ctx context.Context, entry T) (v T, ok bool, err error) {
	entries := []T{entry}
	method, query, arguments, returning, buildErr := specifications.BuildInsertOrUpdate[T](ctx, entries)
	if buildErr != nil {
		err = errors.Warning("sql: insert or update failed").WithCause(buildErr)
		return
	}
	if method == specifications.QueryMethod {
		rows, queryErr := sql.Query(ctx, query, arguments...)
		if queryErr != nil {
			err = errors.Warning("sql: insert or update failed").WithCause(queryErr)
			return
		}

		affected, wErr := specifications.WriteInsertReturning[T](ctx, rows, returning, entries)
		_ = rows.Close()
		if wErr != nil {
			err = errors.Warning("sql: insert or update failed").WithCause(wErr)
			return
		}
		ok = affected == 1
		if ok {
			verErr := specifications.TrySetupAuditVersion[T](ctx, entries)
			if verErr != nil {
				err = errors.Warning("sql: insert or update failed").WithCause(verErr)
				return
			}
			v = entries[0]
		}
	} else {
		result, execErr := sql.Execute(ctx, query, arguments...)
		if execErr != nil {
			err = errors.Warning("sql: insert or update failed").WithCause(execErr)
			return
		}
		ok = result.RowsAffected == 1
		if ok {
			verErr := specifications.TrySetupAuditVersion[T](ctx, entries)
			if verErr != nil {
				err = errors.Warning("sql: insert or update failed").WithCause(verErr)
				return
			}
			v = entry
		}
	}
	return
}

func InsertWhenNotExist[T Table](ctx context.Context, entry T, source conditions.QueryExpr) (v T, ok bool, err error) {
	entries := []T{entry}
	method, query, arguments, returning, buildErr := specifications.BuildInsertWhenNotExist[T](ctx, entries, specifications.QueryExpr{QueryExpr: source})
	if buildErr != nil {
		err = errors.Warning("sql: insert when exist failed").WithCause(buildErr)
		return
	}
	if method == specifications.QueryMethod {
		rows, queryErr := sql.Query(ctx, query, arguments...)
		if queryErr != nil {
			err = errors.Warning("sql: insert when exist failed").WithCause(queryErr)
			return
		}
		affected, wErr := specifications.WriteInsertReturning[T](ctx, rows, returning, entries)
		_ = rows.Close()
		if wErr != nil {
			err = errors.Warning("sql: insert when exist failed").WithCause(wErr)
			return
		}
		ok = affected == 1
		if ok {
			verErr := specifications.TrySetupAuditVersion[T](ctx, entries)
			if verErr != nil {
				err = errors.Warning("sql: insert when exist failed").WithCause(verErr)
				return
			}
			v = entries[0]
		}
	} else {
		result, execErr := sql.Execute(ctx, query, arguments...)
		if execErr != nil {
			err = errors.Warning("sql: insert when exist failed").WithCause(execErr)
			return
		}
		ok = result.RowsAffected == 1
		if ok {
			verErr := specifications.TrySetupAuditVersion[T](ctx, entries)
			if verErr != nil {
				err = errors.Warning("sql: insert when exist failed").WithCause(verErr)
				return
			}
			v = entries[0]
		}
	}
	return
}

func InsertWhenExist[T Table](ctx context.Context, entry T, source conditions.QueryExpr) (v T, ok bool, err error) {
	entries := []T{entry}
	method, query, arguments, returning, buildErr := specifications.BuildInsertWhenExist[T](ctx, entries, specifications.QueryExpr{QueryExpr: source})
	if buildErr != nil {
		err = errors.Warning("sql: insert when not exist failed").WithCause(buildErr)
		return
	}
	if method == specifications.QueryMethod {
		rows, queryErr := sql.Query(ctx, query, arguments...)
		if queryErr != nil {
			err = errors.Warning("sql: insert when not exist failed").WithCause(queryErr)
			return
		}
		affected, wErr := specifications.WriteInsertReturning[T](ctx, rows, returning, entries)
		_ = rows.Close()
		if wErr != nil {
			err = errors.Warning("sql: insert when not exist failed").WithCause(wErr)
			return
		}
		ok = affected == 1
		if ok {
			verErr := specifications.TrySetupAuditVersion[T](ctx, entries)
			if verErr != nil {
				err = errors.Warning("sql: insert when not exist failed").WithCause(verErr)
				return
			}
			v = entries[0]
		}
	} else {
		result, execErr := sql.Execute(ctx, query, arguments...)
		if execErr != nil {
			err = errors.Warning("sql: insert when not exist failed").WithCause(execErr)
			return
		}
		ok = result.RowsAffected == 1
		if ok {
			verErr := specifications.TrySetupAuditVersion[T](ctx, entries)
			if verErr != nil {
				err = errors.Warning("sql: insert when not exist failed").WithCause(verErr)
				return
			}
			v = entries[0]
		}
	}
	return
}
