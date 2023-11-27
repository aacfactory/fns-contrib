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
		table, hasTable := ctx.Localization(query)
		if !hasTable {
			err = errors.Warning("sql: sub query render failed").WithCause(fmt.Errorf("%s was not found in localization", query))
			return
		}
		ctx = With(ctx, query)
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
		_, _ = buf.Write(column)
		_, _ = buf.Write(SPACE)
		_, _ = buf.Write(FORM)
		_, _ = buf.Write(SPACE)
		_, _ = buf.Write(table)
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
