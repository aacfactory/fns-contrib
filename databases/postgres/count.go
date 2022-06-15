package postgres

import (
	"context"
	"github.com/aacfactory/errors"
	"github.com/aacfactory/fns-contrib/databases/sql"
)

func Count(ctx context.Context, cond *Conditions, table Table) (v int, err errors.CodeError) {
	if table == nil {
		err = errors.ServiceError("postgres: count failed for table is nil").WithMeta("postgres", "count")
		return
	}
	tab := createOrLoadTable(table)
	query, args := tab.generateCountSQL(cond)
	// query
	results, queryErr := sql.Query(ctx, query, args...)
	if queryErr != nil {
		err = errors.ServiceError("postgres: count failed").WithCause(queryErr).WithMeta("postgres", "count")
		return
	}
	result, has := results.Next()
	if !has {
		return
	}
	hasColumn, decodeErr := result.Column("_COUNT_", &v)
	if !hasColumn {
		err = errors.ServiceError("postgres: count failed for no named '_COUNT_' column in results").WithMeta("postgres", "count")
		return
	}
	if decodeErr != nil {
		err = errors.ServiceError("postgres: count failed for decoding failed").WithCause(decodeErr).WithMeta("postgres", "count")
		return
	}
	return
}
