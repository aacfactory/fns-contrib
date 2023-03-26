package dal

import (
	"context"
	"github.com/aacfactory/errors"
	"github.com/aacfactory/fns-contrib/databases/sql"
	"strings"
)

const (
	sequenceQL = `SELECT nextval('#name#')`
)

func SequenceNextValue(ctx context.Context, name string) (n int64, err errors.CodeError) {
	rows, queryErr := sql.Query(ctx, strings.Replace(sequenceQL, "#name#", name, 1))
	if queryErr != nil {
		err = errors.Warning("sql: next sequence value failed").WithCause(queryErr).WithMeta("name", name)
		return
	}
	row, ok := rows.Next()
	if !ok {
		err = errors.Warning("sql: next sequence value failed").WithCause(errors.Warning("no affected")).WithMeta("name", name)
		return
	}
	if row.Columns() == nil || len(row.Columns()) == 0 {
		err = errors.Warning("sql: next sequence value failed").WithCause(errors.Warning("no columns")).WithMeta("name", name)
		return
	}
	column := row.Columns()[0]
	v, valueErr := column.Value()
	if valueErr != nil {
		err = errors.Warning("sql: next sequence value failed").WithCause(valueErr).WithMeta("name", name).WithMeta("column", column.Name())
		return
	}
	switch v.(type) {
	case int:
		i := v.(int)
		n = int64(i)
		break
	case int32:
		i := v.(int32)
		n = int64(i)
		break
	case int64:
		n = v.(int64)
		break
	default:
		err = errors.Warning("sql: next sequence value failed").WithCause(errors.Warning("value type is not int")).WithMeta("name", name)
		return
	}
	return
}
