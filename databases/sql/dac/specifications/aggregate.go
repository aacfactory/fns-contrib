package specifications

import (
	"fmt"
	"github.com/aacfactory/errors"
	"github.com/aacfactory/fns-contrib/databases/sql/dac/conditions"
	"github.com/valyala/bytebufferpool"
	"io"
)

type AggregateExpr struct {
	conditions.AggregateExpr
}

func (expr AggregateExpr) Render(ctx Context, w io.Writer) (argument []any, err error) {
	query := expr.Query
	tableNames, hasTableNames := ctx.Localization(query)
	if !hasTableNames {
		err = errors.Warning("sql: aggregate query condition expr render failed").WithCause(fmt.Errorf("%s was not found in localization", query))
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
		err = errors.Warning("sql: aggregate query condition expr render failed").WithCause(fmt.Errorf("%s was not found in localization", expr.Field))
		return
	}
	buf := bytebufferpool.Get()
	defer bytebufferpool.Put(buf)
	_, _ = buf.Write(LB)
	_, _ = buf.Write(SELECT)
	_, _ = buf.Write(SPACE)
	_, _ = buf.WriteString(expr.Func)
	_, _ = buf.Write(LB)
	_, _ = buf.Write(column[0])
	_, _ = buf.Write(RB)
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
			err = errors.Warning("sql: aggregate query condition expr render failed").WithCause(err)
			return
		}
	}
	_, _ = buf.Write(RB)
	_, err = w.Write(buf.Bytes())
	if err != nil {
		err = errors.Warning("sql: aggregate query condition expr render failed").WithCause(err)
		return
	}
	return
}
