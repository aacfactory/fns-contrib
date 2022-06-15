package postgres

import (
	"context"
	"github.com/aacfactory/errors"
	"github.com/aacfactory/fns-contrib/databases/sql"
)

func Exist(ctx context.Context, cond *Conditions, table Table) (has bool, err errors.CodeError) {
	if table == nil {
		err = errors.ServiceError("postgres: exist failed for table is nil").WithMeta("postgres", "exist")
		return
	}
	tab := createOrLoadTable(table)
	query, args := tab.generateExistSQL(cond)
	// query
	results, queryErr := sql.Query(ctx, query, args...)
	if queryErr != nil {
		err = errors.ServiceError("postgres: exist failed").WithCause(queryErr).WithMeta("postgres", "exist")
		return
	}
	has = !results.Empty()
	return
}
