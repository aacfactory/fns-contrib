package sql

import (
	db "database/sql"
	"fmt"
	"github.com/aacfactory/errors"
	"github.com/aacfactory/fns"
	"strings"
	"time"
)

func (svc *_service) executeFn(ctx fns.Context, param Param) (result *ExecResult, err errors.CodeError) {
	query := strings.TrimSpace(param.Query)
	if query == "" {
		err = errors.ServiceError("fns SQL: execute failed for no query string")
		_ = svc.txRollback(ctx)
		return
	}

	exec := svc.getExecutor(ctx)

	var startTime time.Time
	if svc.enableDebugLog && ctx.App().Log().DebugEnabled() {
		startTime = time.Now()
	}
	var dbResult db.Result
	if param.Args == nil {
		dbResult0, execErr := exec.ExecContext(ctx, query)
		if execErr != nil {
			err = errors.ServiceError("fns SQL: execute failed").WithCause(execErr)
			_ = svc.txRollback(ctx)
			return
		}
		dbResult = dbResult0
	} else {
		args := param.Args.mapToSQLArgs()
		dbResult0, execErr := exec.ExecContext(ctx, query, args...)
		if execErr != nil {
			err = errors.ServiceError("fns SQL: execute failed").WithCause(execErr)
			_ = svc.txRollback(ctx)
			return
		}
		dbResult = dbResult0
	}
	if svc.enableDebugLog && ctx.App().Log().DebugEnabled() {
		latency := time.Now().Sub(startTime)
		ctx.App().Log().Debug().With("sql", "execute").With("sql_latency", latency.String()).Message(fmt.Sprintf("\n%s\n", query))
	}

	lastInsertId, _ := dbResult.LastInsertId()
	affected, _ := dbResult.RowsAffected()

	result = &ExecResult{
		Affected:     affected,
		LastInsertId: lastInsertId,
	}

	return
}
