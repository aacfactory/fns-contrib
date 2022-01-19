package postgres

import (
	"github.com/aacfactory/errors"
	"github.com/aacfactory/fns"
	"github.com/aacfactory/fns-contrib/databases/sql"
)

func Exist(ctx fns.Context, cond *Conditions, table Table) (has bool, err errors.CodeError) {
	if table == nil {
		err = errors.ServiceError("fns Postgres: exist failed for table is nil").WithMeta("_fns_postgres", "Exist")
		return
	}
	tab := createOrLoadTable(table)
	query, args := tab.generateExistSQL(cond)
	// query
	results, queryErr := sql.Query(ctx, sql.Param{
		Query: query,
		Args:  args,
	})
	if queryErr != nil {
		err = errors.ServiceError("fns Postgres: exist failed").WithCause(queryErr).WithMeta("_fns_postgres", "Exist")
		return
	}
	has = !results.Empty()
	return
}
