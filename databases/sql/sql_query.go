package sql

import (
	db "database/sql"
	"github.com/aacfactory/errors"
	"github.com/aacfactory/fns"
	"strings"
)

func (svc *Service) queryFn(ctx fns.Context, param Param) (rows *Rows, err errors.CodeError) {
	query := strings.TrimSpace(param.Query)
	if query == "" {
		err = errors.ServiceError("fns SQL: query failed for no query string")
		return
	}
	var q QueryAble
	if param.InTx {
		tx, hasTx := svc.getTx(ctx)
		if !hasTx {
			err = errors.ServiceError("fns SQL: query in tx failed cause tx was not found")
			return
		}
		q = tx
	}
	q = svc.client.Reader()

	var dbRows *db.Rows
	if param.Args == nil {
		dbRows0, queryErr := q.QueryContext(ctx, query)
		if queryErr != nil {
			err = errors.ServiceError("fns SQL: query failed").WithCause(queryErr)
			return
		}
		dbRows = dbRows0
	} else {
		args := param.Args.mapToSQLArgs()
		dbRows0, queryErr := q.QueryContext(ctx, query, args...)
		if queryErr != nil {
			err = errors.ServiceError("fns SQL: query failed").WithCause(queryErr)
			return
		}
		dbRows = dbRows0
	}

	rows0, rowErr := NewRows(dbRows)
	if rowErr != nil {
		err = errors.ServiceError("fns SQL: query failed").WithCause(rowErr)
		return
	}
	rows = rows0
	return
}
