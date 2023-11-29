package dac

import (
	"github.com/aacfactory/errors"
	"github.com/aacfactory/fns-contrib/databases/sql"
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
		err = errors.Warning("sql: insert failed").WithCause(buildErr)
		return
	}
	if method == specifications.QueryMethod {
		rows, queryErr := sql.Query(ctx, query, arguments...)
		if queryErr != nil {
			err = errors.Warning("sql: insert failed").WithCause(queryErr)
			return
		}
		affected, err = specifications.WriteInsertReturning[T](ctx, rows, returning, entries)
		_ = rows.Close()
		if err != nil {
			err = errors.Warning("sql: insert failed").WithCause(err)
			return
		}
	} else {
		result, execErr := sql.Execute(ctx, query, arguments...)
		if execErr != nil {
			err = errors.Warning("sql: insert failed").WithCause(execErr)
			return
		}
		affected = result.RowsAffected
	}
	return
}

func InsertOrUpdate[T Table](ctx context.Context, entry T) (v T, err error) {

	return
}

func InsertWhenNotExist[T Table](ctx context.Context, entry T) (v T, err error) {

	return
}

func InsertWhenExist[T Table](ctx context.Context, entry T) (v T, err error) {

	return
}
