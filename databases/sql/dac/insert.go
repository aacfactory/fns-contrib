package dac

import (
	"github.com/aacfactory/errors"
	"github.com/aacfactory/fns-contrib/databases/sql"
	"github.com/aacfactory/fns-contrib/databases/sql/dac/conditions"
	"github.com/aacfactory/fns-contrib/databases/sql/dac/specifications"
	"github.com/aacfactory/fns/context"
)

func Insert[T Table](ctx context.Context, entry T) (v T, ok bool, err error) {
	method, query, arguments, returning, buildErr := specifications.BuildInsert[T](ctx, entry)
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
		entries := []T{entry}
		affected, wErr := specifications.WriteInsertReturning[T](ctx, rows, returning, entries)
		_ = rows.Close()
		if wErr != nil {
			err = errors.Warning("sql: insert failed").WithCause(wErr)
			return
		}
		ok = affected > 0
		if ok {
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
			v = entry
		}
	}

	return
}

func InsertMulti[T Table](ctx context.Context, entries []T) (affected int64, err error) {
	if len(entries) == 0 {
		return
	}
	method, query, arguments, returning, buildErr := specifications.BuildInsert[T](ctx, entries...)
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
	} else {
		result, execErr := sql.Execute(ctx, query, arguments...)
		if execErr != nil {
			err = errors.Warning("sql: insert multi failed").WithCause(execErr)
			return
		}
		affected = result.RowsAffected
	}
	return
}

func InsertOrUpdate[T Table](ctx context.Context, entry T) (v T, err error) {
	method, query, arguments, returning, buildErr := specifications.BuildInsertOrUpdate[T](ctx, entry)
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
		entries := []T{entry}
		affected, wErr := specifications.WriteInsertReturning[T](ctx, rows, returning, entries)
		_ = rows.Close()
		if wErr != nil {
			err = errors.Warning("sql: insert or update failed").WithCause(wErr)
			return
		}
		if affected == 1 {
			v = entries[0]
		}
	} else {
		result, execErr := sql.Execute(ctx, query, arguments...)
		if execErr != nil {
			err = errors.Warning("sql: insert or update failed").WithCause(execErr)
			return
		}
		if result.RowsAffected == 1 {
			v = entry
		}
	}
	return
}

func InsertWhenNotExist[T Table](ctx context.Context, entry T, source conditions.QueryExpr) (v T, err error) {
	method, query, arguments, returning, buildErr := specifications.BuildInsertWhenExist[T](ctx, entry, specifications.QueryExpr{QueryExpr: source})
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
		entries := []T{entry}
		affected, wErr := specifications.WriteInsertReturning[T](ctx, rows, returning, entries)
		_ = rows.Close()
		if wErr != nil {
			err = errors.Warning("sql: insert when exist failed").WithCause(wErr)
			return
		}
		if affected == 1 {
			v = entries[0]
		}
	} else {
		result, execErr := sql.Execute(ctx, query, arguments...)
		if execErr != nil {
			err = errors.Warning("sql: insert when exist failed").WithCause(execErr)
			return
		}
		if result.RowsAffected == 1 {
			v = entry
		}
	}
	return
}

func InsertWhenExist[T Table](ctx context.Context, entry T, source conditions.QueryExpr) (v T, err error) {
	method, query, arguments, returning, buildErr := specifications.BuildInsertWhenNotExist[T](ctx, entry, specifications.QueryExpr{QueryExpr: source})
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
		entries := []T{entry}
		affected, wErr := specifications.WriteInsertReturning[T](ctx, rows, returning, entries)
		_ = rows.Close()
		if wErr != nil {
			err = errors.Warning("sql: insert when not exist failed").WithCause(wErr)
			return
		}
		if affected == 1 {
			v = entries[0]
		}
	} else {
		result, execErr := sql.Execute(ctx, query, arguments...)
		if execErr != nil {
			err = errors.Warning("sql: insert when not exist failed").WithCause(execErr)
			return
		}
		if result.RowsAffected == 1 {
			v = entry
		}
	}
	return
}
