package specifications

import (
	"bytes"
	"database/sql"
	"fmt"
	"github.com/aacfactory/errors"
	"github.com/aacfactory/fns-contrib/databases/sql/dac/conditions"
	"github.com/valyala/bytebufferpool"
	"io"
)

type Predicate struct {
	conditions.Predicate
}

func (p Predicate) Render(ctx Context, w io.Writer) (argument []any, err error) {
	buf := bytebufferpool.Get()
	defer bytebufferpool.Put(buf)
	column, hasColumn := ctx.Localization(p.Field)
	if !hasColumn {
		err = errors.Warning("sql: predicate render failed").WithCause(fmt.Errorf("%s was not found in localization", p.Field))
		return
	}
	_, _ = buf.Write(column[0])
	_, _ = buf.Write(SPACE)
	_, _ = buf.Write(p.Operator.Bytes())
	_, _ = buf.Write(SPACE)

	switch expr := p.Expression.(type) {
	case conditions.Literal:
		_, _ = buf.Write(expr.Bytes())
		break
	case sql.NamedArg:
		_, _ = buf.Write(AT)
		_, _ = buf.WriteString(expr.Name)
		argument = append(argument, expr)
		break
	case conditions.QueryExpr:
		sub, subErr := QueryExpr{expr}.Render(ctx, buf)
		if subErr != nil {
			err = errors.Warning("sql: predicate render failed").WithCause(subErr)
			return
		}
		argument = append(argument, sub...)
		break
	case []any:
		if p.Operator != conditions.BETWEEN && p.Operator != conditions.IN && p.Operator != conditions.NOTIN {
			err = errors.Warning("sql: predicate render failed").WithCause(fmt.Errorf("%s only can has one expression", p.Field))
			return
		}
		exprLen := len(expr)
		if exprLen == 0 {
			err = errors.Warning("sql: predicate render failed").WithCause(fmt.Errorf("%s only can has no expression", p.Field))
			return
		}
		if exprLen == 1 {
			queryExpr, isQueryExpr := expr[0].(QueryExpr)
			if isQueryExpr {
				sub, subErr := queryExpr.Render(ctx, buf)
				if subErr != nil {
					err = errors.Warning("sql: predicate render failed").WithCause(subErr)
					return
				}
				argument = append(argument, sub...)
				break
			}
		}

		exprs := make([][]byte, 0, len(expr))
		for _, e := range expr {
			switch se := e.(type) {
			case conditions.Literal:
				exprs = append(exprs, se.Bytes())
				break
			case sql.NamedArg:
				err = errors.Warning("sql: predicate render failed").WithCause(fmt.Errorf("%s only can has named arg", p.Field))
				return
			case conditions.QueryExpr:
				sbb := bytebufferpool.Get()
				sub, subErr := QueryExpr{se}.Render(ctx, sbb)
				if subErr != nil {
					bytebufferpool.Put(sbb)
					err = errors.Warning("sql: predicate render failed").WithCause(subErr)
					return
				}
				subQuery := sbb.Bytes()
				bytebufferpool.Put(sbb)
				if len(expr) == 1 {
					subQuery = subQuery[bytes.IndexByte(subQuery, '(')+1 : bytes.LastIndexByte(subQuery, ')')]
				}
				exprs = append(exprs, subQuery)
				argument = append(argument, sub...)
				break
			default:
				exprs = append(exprs, ctx.NextQueryPlaceholder())
				argument = append(argument, se)
				break
			}
		}

		_, _ = buf.Write(LB)
		_, _ = buf.Write(bytes.Join(exprs, COMMA))
		_, _ = buf.Write(RB)
		break
	default:

		_, _ = buf.Write(ctx.NextQueryPlaceholder())
		argument = append(argument, expr)
		break
	}

	_, err = w.Write(buf.Bytes())
	if err != nil {
		err = errors.Warning("sql: predicate render failed").WithCause(err)
		return
	}
	return
}
