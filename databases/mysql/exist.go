package mysql

import (
	"context"
	"github.com/aacfactory/errors"
	"github.com/aacfactory/fns-contrib/databases/sql"
)

func Exist(ctx context.Context, cond *Conditions, table Table) (has bool, err errors.CodeError) {
	if table == nil {
		err = errors.ServiceError("mysql: exist failed for table is nil").WithMeta("mysql", "exist")
		return
	}
	tab := createOrLoadTable(table)
	query, args := tab.generateExistSQL(cond)
	// query
	results, queryErr := sql.Query(ctx, query, args...)
	if queryErr != nil {
		err = errors.ServiceError("mysql: exist failed").WithCause(queryErr).WithMeta("mysql", "exist")
		return
	}
	has = !results.Empty()
	return
}
