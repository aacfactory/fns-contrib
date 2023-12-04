package specifications

import (
	"fmt"
	"github.com/aacfactory/errors"
	"github.com/aacfactory/fns-contrib/databases/sql/dac/conditions"
	"github.com/valyala/bytebufferpool"
	"io"
)

type QueryExpr struct {
	conditions.QueryExpr
}

func (expr QueryExpr) Render(ctx Context, w io.Writer) (argument []any, err error) {
	switch query := expr.Query.(type) {
	case string:
		if expr.Cond.Exist() {
			err = errors.Warning("sql: sub query render failed").WithCause(fmt.Errorf("literal sub query can not has condition"))
			return
		}
		buf := bytebufferpool.Get()
		defer bytebufferpool.Put(buf)
		_, _ = buf.Write(LB)
		_, _ = buf.WriteString(query)
		_, _ = buf.Write(RB)
		_, err = w.Write(buf.Bytes())
		if err != nil {
			err = errors.Warning("sql: sub query render failed").WithCause(err)
			return
		}
		break
	default:
		tableNames, hasTableNames := ctx.Localization(query)
		if !hasTableNames {
			err = errors.Warning("sql: sub query render failed").WithCause(fmt.Errorf("%s was not found in localization", query))
			return
		}
		tableName := tableNames[0]
		if len(tableNames) == 2 {
			tableName = append(tableNames[0], '.')
			tableName = append(tableName, tableNames[1]...)
		}
		ctx = withTable(ctx, query)
		column, hasColumn := ctx.Localization(expr.Field)
		if !hasColumn {
			err = errors.Warning("sql: sub query render failed").WithCause(fmt.Errorf("%s was not found in localization", expr.Field))
			return
		}
		buf := bytebufferpool.Get()
		defer bytebufferpool.Put(buf)
		_, _ = buf.Write(LB)
		_, _ = buf.Write(SELECT)
		_, _ = buf.Write(SPACE)
		if expr.Aggregate == "" {
			_, _ = buf.Write(column[0])
		} else {
			_, _ = buf.WriteString(expr.Aggregate)
			_, _ = buf.Write(LB)
			_, _ = buf.Write(column[0])
			_, _ = buf.Write(RB)
		}
		_, _ = buf.Write(SPACE)
		_, _ = buf.Write(FORM)
		_, _ = buf.Write(SPACE)
		_, _ = buf.Write(tableName)
		if expr.Cond.Exist() {
			_, _ = buf.Write(SPACE)
			_, _ = buf.Write(WHERE)
			_, _ = buf.Write(SPACE)
			argument, err = Condition{expr.Cond}.Render(ctx, buf)
			if err != nil {
				err = errors.Warning("sql: sub query render failed").WithCause(err)
				return
			}
		}
		_, _ = buf.Write(RB)
		_, err = w.Write(buf.Bytes())
		if err != nil {
			err = errors.Warning("sql: sub query render failed").WithCause(err)
			return
		}
		break
	}
	return
}
