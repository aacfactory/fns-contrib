package mysql

import (
	"context"
	"github.com/aacfactory/errors"
	"github.com/aacfactory/fns-contrib/databases/sql"
)

func Count(ctx context.Context, cond *Conditions, table Table) (v int, err errors.CodeError) {
	if table == nil {
		err = errors.ServiceError("mysql: count failed for table is nil").WithMeta("mysql", "count")
		return
	}
	tab := createOrLoadTable(table)
	query, args := tab.generateCountSQL(cond)
	// query
	results, queryErr := sql.Query(ctx, query, args...)
	if queryErr != nil {
		err = errors.ServiceError("mysql: count failed").WithCause(queryErr).WithMeta("mysql", "count")
		return
	}
	result, has := results.Next()
	if !has {
		return
	}
	hasColumn, decodeErr := result.Column("_COUNT_", &v)
	if !hasColumn {
		err = errors.ServiceError("mysql: count failed for no named '_COUNT_' column in results").WithMeta("mysql", "count")
		return
	}
	if decodeErr != nil {
		err = errors.ServiceError("mysql: count failed for decoding failed").WithCause(decodeErr).WithMeta("mysql", "count")
		return
	}
	return
}
