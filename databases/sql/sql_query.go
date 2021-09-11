package sql

import (
	db "database/sql"
	"github.com/aacfactory/errors"
	"github.com/aacfactory/fns"
	"strings"
)

func (svc *_service) queryFn(ctx fns.Context, param Param) (rows *Rows, err errors.CodeError) {
	query := strings.TrimSpace(param.Query)
	if query == "" {
		err = errors.ServiceError("fns SQL: query failed for no query string")
		svc.txRollbackIfHas(ctx)
		return
	}

	q := svc.getQueryAble(ctx)

	var dbRows *db.Rows
	if param.Args == nil {
		dbRows0, queryErr := q.QueryContext(ctx, query)
		if queryErr != nil {
			err = errors.ServiceError("fns SQL: query failed").WithCause(queryErr)
			svc.txRollbackIfHas(ctx)
			return
		}
		dbRows = dbRows0
	} else {
		args := param.Args.mapToSQLArgs()
		dbRows0, queryErr := q.QueryContext(ctx, query, args...)
		if queryErr != nil {
			err = errors.ServiceError("fns SQL: query failed").WithCause(queryErr)
			svc.txRollbackIfHas(ctx)
			return
		}
		dbRows = dbRows0
	}

	rows0, rowErr := NewRows(dbRows)
	if rowErr != nil {
		err = errors.ServiceError("fns SQL: query failed").WithCause(rowErr)
		svc.txRollbackIfHas(ctx)
		return
	}
	rows = rows0
	return
}
