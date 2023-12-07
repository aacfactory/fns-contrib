package specifications

import (
	"bytes"
	"database/sql"
	"fmt"
	"github.com/aacfactory/errors"
	"github.com/aacfactory/fns-contrib/databases/sql/dac/conditions"
	"github.com/aacfactory/fns/commons/bytex"
	"github.com/valyala/bytebufferpool"
	"io"
	"strings"
)

type Predicate struct {
	conditions.Predicate
}

func (p Predicate) Render(ctx Context, w io.Writer) (argument []any, err error) {
	column, hasColumn := ctx.Localization(p.Field)
	if !hasColumn {
		err = errors.Warning("sql: predicate render failed").WithCause(fmt.Errorf("%s was not found in localization", p.Field))
		return
	}
	_, _ = w.Write(bytex.FromString(column[0]))
	_, _ = w.Write(SPACE)
	_, _ = w.Write(bytex.FromString(p.Operator.String()))
	_, _ = w.Write(SPACE)

	switch expr := p.Expression.(type) {
	case conditions.Literal:
		_, _ = w.Write(bytex.FromString(expr.String()))
		break
	case sql.NamedArg:
		_, _ = w.Write(AT)
		_, _ = w.Write(bytex.FromString(expr.Name))
		argument = append(argument, expr)
		break
	case conditions.QueryExpr:
		sub, subErr := QueryExpr{expr}.Render(ctx, w)
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
				sub, subErr := queryExpr.Render(ctx, w)
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
				exprs = append(exprs, bytex.FromString(se.String()))
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
				subQuery := sbb.String()
				bytebufferpool.Put(sbb)
				if len(expr) == 1 {
					subQuery = subQuery[strings.IndexByte(subQuery, '(')+1 : strings.LastIndexByte(subQuery, ')')]
				}
				exprs = append(exprs, bytex.FromString(subQuery))
				argument = append(argument, sub...)
				break
			default:
				exprs = append(exprs, bytex.FromString(ctx.NextQueryPlaceholder()))
				argument = append(argument, se)
				break
			}
		}

		_, _ = w.Write(LB)
		_, _ = w.Write(bytes.Join(exprs, COMMA))
		_, _ = w.Write(RB)
		break
	default:
		_, _ = w.Write(bytex.FromString(ctx.NextQueryPlaceholder()))
		argument = append(argument, expr)
		break
	}

	return
}
