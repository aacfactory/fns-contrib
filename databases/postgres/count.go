package postgres

import (
	"github.com/aacfactory/errors"
	"github.com/aacfactory/fns"
	"github.com/aacfactory/fns-contrib/databases/sql"
)

func Count(ctx fns.Context, cond *Conditions, table Table) (v int, err errors.CodeError) {
	if table == nil {
		err = errors.ServiceError("fns Postgres: count failed for table is nil").WithMeta("_fns_postgres", "Count")
		return
	}
	tab := createOrLoadTable(table)
	query, args := tab.generateCountSQL(cond)
	// query
	results, queryErr := sql.Query(ctx, sql.Param{
		Query: query,
		Args:  args,
	})
	if queryErr != nil {
		err = queryErr
		return
	}
	result, has := results.Next()
	if !has {
		return
	}

	hasColumn, decodeErr := result.Column("_COUNT_", &v)
	if !hasColumn {
		err = errors.ServiceError("fns Postgres: count failed for no named '_COUNT_' column in results").WithMeta("_fns_postgres", "Count")
		return
	}
	if decodeErr != nil {
		err = errors.ServiceError("fns Postgres: count failed for decoding failed").WithCause(decodeErr).WithMeta("_fns_postgres", "Count")
		return
	}
	return
}
