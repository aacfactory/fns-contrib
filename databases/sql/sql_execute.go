package sql

import (
	db "database/sql"
	"github.com/aacfactory/errors"
	"github.com/aacfactory/fns"
	"strings"
)

func (svc *Service) executeFn(ctx fns.Context, param Param) (result *ExecResult, err errors.CodeError) {
	query := strings.TrimSpace(param.Query)
	if query == "" {
		err = errors.ServiceError("fns SQL: execute failed for no query string")
		return
	}
	var exec Executor
	if param.InTx {
		tx, hasTx := svc.getTx(ctx)
		if !hasTx {
			err = errors.ServiceError("fns SQL: execute in tx failed cause tx was not found")
			return
		}
		exec = tx
	}
	exec = svc.client.Writer()

	var dbResult db.Result
	if param.Args == nil {
		dbResult0, execErr := exec.ExecContext(ctx, query)
		if execErr != nil {
			err = errors.ServiceError("fns SQL: execute failed").WithCause(execErr)
			return
		}
		dbResult = dbResult0
	} else {
		args := param.Args.mapToSQLArgs()
		dbResult0, execErr := exec.ExecContext(ctx, query, args...)
		if execErr != nil {
			err = errors.ServiceError("fns SQL: execute failed").WithCause(execErr)
			return
		}
		dbResult = dbResult0
	}

	lastInsertId, _ := dbResult.LastInsertId()
	affected, _ := dbResult.RowsAffected()

	result = &ExecResult{
		Affected:     affected,
		LastInsertId: lastInsertId,
	}

	return
}
