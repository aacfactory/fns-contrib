package dac

import (
	"github.com/aacfactory/errors"
	"github.com/aacfactory/fns-contrib/databases/sql"
	"github.com/aacfactory/fns-contrib/databases/sql/dac/conditions"
	"github.com/aacfactory/fns-contrib/databases/sql/dac/specifications"
	"github.com/aacfactory/fns/context"
)

func Delete[T Table](ctx context.Context, entry T) (v T, ok bool, err error) {
	entries := []T{entry}
	_, query, arguments, buildErr := specifications.BuildDelete[T](ctx, entries)
	if buildErr != nil {
		err = errors.Warning("sql: delete failed").WithCause(buildErr)
		return
	}

	result, execErr := sql.Execute(ctx, query, arguments...)
	if execErr != nil {
		err = errors.Warning("sql: delete failed").WithCause(execErr)
		return
	}

	if ok = result.RowsAffected == 1; ok {
		verErr := specifications.TrySetupAuditVersion[T](ctx, entries)
		if verErr != nil {
			err = errors.Warning("sql: delete failed").WithCause(verErr)
			return
		}
		v = entry
	}
	return
}

func DeleteByCondition[T Table](ctx context.Context, cond conditions.Condition) (affected int64, err error) {
	_, query, arguments, buildErr := specifications.BuildDeleteByCondition[T](ctx, specifications.Condition{Condition: cond})
	if buildErr != nil {
		err = errors.Warning("sql: delete by condition failed").WithCause(buildErr)
		return
	}

	result, execErr := sql.Execute(ctx, query, arguments...)
	if execErr != nil {
		err = errors.Warning("sql: delete by condition failed").WithCause(execErr)
		return
	}

	affected = result.RowsAffected
	return
}
