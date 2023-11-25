package dal

import (
	"github.com/aacfactory/errors"
	"github.com/aacfactory/fns-contrib/databases/sql"
	"github.com/aacfactory/fns/context"
	"strings"
)

const (
	sequenceQL = `SELECT nextval('#name#')`
)

func SequenceNextValue(ctx context.Context, name string) (n int64, err error) {
	rows, queryErr := sql.Query(ctx, strings.Replace(sequenceQL, "#name#", name, 1))
	if queryErr != nil {
		err = errors.Warning("sql: next sequence value failed").WithCause(queryErr).WithMeta("name", name)
		return
	}
	ok := rows.Next()
	if !ok {
		err = errors.Warning("sql: next sequence value failed").WithCause(errors.Warning("no affected")).WithMeta("name", name)
		return
	}
	scanErr := rows.Scan(&n)
	if scanErr != nil {
		err = errors.Warning("sql: next sequence value failed").WithCause(scanErr).WithMeta("name", name)
		return
	}
	return
}
