package specifications

import (
	"fmt"
	"github.com/aacfactory/errors"
	"github.com/aacfactory/fns-contrib/databases/sql/dac/conditions"
	"github.com/aacfactory/fns/commons/bytex"
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
		_, _ = w.Write(LB)
		_, _ = w.Write(bytex.FromString(query))
		_, _ = w.Write(RB)
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
			tableName = fmt.Sprintf("%s.%s", tableNames[0], tableNames[1])
		}
		ctx = SwitchKey(ctx, query)
		column, hasColumn := ctx.Localization(expr.Field)
		if !hasColumn {
			err = errors.Warning("sql: sub query render failed").WithCause(fmt.Errorf("%s was not found in localization", expr.Field))
			return
		}
		_, _ = w.Write(LB)
		_, _ = w.Write(SELECT)
		_, _ = w.Write(SPACE)
		if expr.Aggregate == "" {
			_, _ = w.Write(bytex.FromString(column[0]))
		} else {
			_, _ = w.Write(bytex.FromString(expr.Aggregate))
			_, _ = w.Write(LB)
			_, _ = w.Write(bytex.FromString(column[0]))
			_, _ = w.Write(RB)
		}
		_, _ = w.Write(SPACE)
		_, _ = w.Write(FROM)
		_, _ = w.Write(SPACE)
		_, _ = w.Write(bytex.FromString(tableName))
		if expr.Cond.Exist() {
			_, _ = w.Write(SPACE)
			_, _ = w.Write(WHERE)
			_, _ = w.Write(SPACE)
			argument, err = Condition{expr.Cond}.Render(ctx, w)
			if err != nil {
				err = errors.Warning("sql: sub query render failed").WithCause(err)
				return
			}
		}
		_, _ = w.Write(RB)
		break
	}
	return
}
