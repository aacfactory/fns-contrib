package sql

import (
	db "database/sql"
	"fmt"
	"github.com/aacfactory/errors"
	"github.com/aacfactory/fns"
	"strings"
	"time"
)

func (svc *_service) queryFn(ctx fns.Context, param Param) (rows *Rows, err errors.CodeError) {
	query := strings.TrimSpace(param.Query)
	if query == "" {
		err = errors.ServiceError("fns SQL: query failed for no query string")
		_ = svc.txRollback(ctx)
		return
	}

	q := svc.getQueryAble(ctx)
	var startTime time.Time
	if svc.enableDebugLog && ctx.App().Log().DebugEnabled() {
		startTime = time.Now()
	}
	var dbRows *db.Rows
	if param.Args == nil {
		dbRows0, queryErr := q.QueryContext(ctx, query)
		if queryErr != nil {
			err = errors.ServiceError("fns SQL: query failed").WithCause(queryErr)
			if svc.enableDebugLog && ctx.App().Log().DebugEnabled() {
				ctx.App().Log().Debug().Message(fmt.Sprintf("%+v", err.WithMeta("query", query)))
			}
			_ = svc.txRollback(ctx)
			return
		}
		dbRows = dbRows0
	} else {
		args := param.Args.mapToSQLArgs()
		dbRows0, queryErr := q.QueryContext(ctx, query, args...)
		if queryErr != nil {
			err = errors.ServiceError("fns SQL: query failed").WithCause(queryErr)
			if svc.enableDebugLog && ctx.App().Log().DebugEnabled() {
				ctx.App().Log().Debug().Message(fmt.Sprintf("%+v", err.WithMeta("query", query)))
			}
			_ = svc.txRollback(ctx)
			return
		}
		dbRows = dbRows0
	}
	if svc.enableDebugLog && ctx.App().Log().DebugEnabled() {
		latency := time.Now().Sub(startTime)
		ctx.App().Log().Debug().With("sql_latency", latency.String()).Message(fmt.Sprintf("\n%s\n", query))
	}
	rows0, rowErr := NewRows(dbRows)
	if rowErr != nil {
		err = errors.ServiceError("fns SQL: query failed").WithCause(rowErr)
		_ = svc.txRollback(ctx)
		return
	}
	rows = rows0
	return
}
