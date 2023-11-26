package conditions

import (
	"fmt"
	"github.com/aacfactory/errors"
	"github.com/aacfactory/fns-contrib/databases/sql/dac/languages"
	"github.com/valyala/bytebufferpool"
	"io"
)

func Query(query any, field string, cond Condition) QueryExpr {
	return QueryExpr{
		query: query,
		field: field,
		cond:  cond,
	}
}

func LitQuery(query string) QueryExpr {
	return QueryExpr{
		query: query,
		field: "",
		cond:  Condition{},
	}
}

type QueryExpr struct {
	query any
	field string
	cond  Condition
}

func (expr QueryExpr) Render(ctx Context, w io.Writer) (argument []any, err error) {
	switch query := expr.query.(type) {
	case string:
		if expr.cond.Exist() {
			err = errors.Warning("sql: sub query render failed").WithCause(fmt.Errorf("literal sub query can not has condition"))
			return
		}
		buf := bytebufferpool.Get()
		defer bytebufferpool.Put(buf)
		_, _ = buf.Write(languages.LB)
		_, _ = buf.WriteString(query)
		_, _ = buf.Write(languages.RB)
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
		column, hasColumn := ctx.Localization(expr.field)
		if !hasColumn {
			err = errors.Warning("sql: sub query render failed").WithCause(fmt.Errorf("%s was not found in localization", expr.field))
			return
		}
		buf := bytebufferpool.Get()
		defer bytebufferpool.Put(buf)
		_, _ = buf.Write(languages.LB)
		_, _ = buf.Write(languages.SELECT)
		_, _ = buf.Write(languages.SPACE)
		_, _ = buf.Write(column)
		_, _ = buf.Write(languages.SPACE)
		_, _ = buf.Write(languages.FORM)
		_, _ = buf.Write(languages.SPACE)
		_, _ = buf.Write(table)
		if expr.cond.Exist() {
			_, _ = buf.Write(languages.SPACE)
			_, _ = buf.Write(languages.WHERE)
			_, _ = buf.Write(languages.SPACE)
			argument, err = expr.cond.Render(ctx, buf)
			if err != nil {
				err = errors.Warning("sql: sub query render failed").WithCause(err)
				return
			}
		}
		_, _ = buf.Write(languages.RB)
		_, err = w.Write(buf.Bytes())
		if err != nil {
			err = errors.Warning("sql: sub query render failed").WithCause(err)
			return
		}
		break
	}
	return
}
