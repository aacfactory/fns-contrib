package conditions

import (
	"bytes"
	"database/sql"
	"fmt"
	"github.com/aacfactory/errors"
	"github.com/aacfactory/fns-contrib/databases/sql/dac/languages"
	"github.com/valyala/bytebufferpool"
	"io"
)

func Eq(field string, expression any) Predicate {
	return Predicate{
		Field:      field,
		Operator:   Equal,
		Expression: expression,
	}
}

func NotEq(field string, expression any) Predicate {
	return Predicate{
		Field:      field,
		Operator:   NotEqual,
		Expression: expression,
	}
}

func Gt(field string, expression any) Predicate {
	return Predicate{
		Field:      field,
		Operator:   GreatThan,
		Expression: expression,
	}
}

func Gte(field string, expression any) Predicate {
	return Predicate{
		Field:      field,
		Operator:   GreatThanOrEqual,
		Expression: expression,
	}
}

func Lt(field string, expression any) Predicate {
	return Predicate{
		Field:      field,
		Operator:   LessThan,
		Expression: expression,
	}
}

func Lte(field string, expression any) Predicate {
	return Predicate{
		Field:      field,
		Operator:   LessThanOrEqual,
		Expression: expression,
	}
}

func Between(field string, left any, right any) Predicate {
	return Predicate{
		Field:      field,
		Operator:   BETWEEN,
		Expression: []any{left, right},
	}
}

func In(field string, expression ...any) Predicate {
	return Predicate{
		Field:      field,
		Operator:   IN,
		Expression: expression,
	}
}

func NotIn(field string, expression ...any) Predicate {
	return Predicate{
		Field:      field,
		Operator:   NOTIN,
		Expression: expression,
	}
}

func Like(field string, expression string) Predicate {
	return Predicate{
		Field:      field,
		Operator:   LIKE,
		Expression: String(expression + "%"),
	}
}

func LikeLast(field string, expression string) Predicate {
	return Predicate{
		Field:      field,
		Operator:   LIKE,
		Expression: String("%" + expression),
	}
}

func LikeContains(field string, expression string) Predicate {
	return Predicate{
		Field:      field,
		Operator:   LIKE,
		Expression: String("%" + expression + "%"),
	}
}

type Predicate struct {
	Field      string
	Operator   Operator
	Expression any
}

func (p Predicate) Render(ctx RenderContext, w io.Writer) (argument []any, err error) {
	buf := bytebufferpool.Get()
	defer bytebufferpool.Put(buf)
	column, hasColumn := ctx.Localization(p.Field)
	if !hasColumn {
		err = errors.Warning("sql: predicate render failed").WithCause(fmt.Errorf("%s was not found in localization", p.Field))
		return
	}
	_, _ = buf.Write(column)
	_, _ = buf.Write(languages.SPACE)
	_, _ = buf.Write(p.Operator.Bytes())

	_, _ = buf.Write(p.Operator.Bytes())
	_, _ = buf.Write(languages.SPACE)

	switch expr := p.Expression.(type) {
	case Literal:
		_, _ = buf.Write(expr.Bytes())
		break
	case sql.NamedArg:
		_, _ = buf.Write(languages.AT)
		_, _ = buf.WriteString(expr.Name)
		argument = append(argument, expr)
		break
	case QueryExpr:
		sub, subErr := expr.Render(ctx, buf)
		if subErr != nil {
			err = errors.Warning("sql: predicate render failed").WithCause(subErr)
			return
		}
		argument = append(argument, sub...)
		break
	case []any:
		if p.Operator != BETWEEN && p.Operator != IN && p.Operator != NOTIN {
			err = errors.Warning("sql: predicate render failed").WithCause(fmt.Errorf("%s only can has one expression", p.Field))
			return
		}
		exprs := make([][]byte, 0, len(expr))
		for _, e := range expr {
			switch se := e.(type) {
			case Literal:
				exprs = append(exprs, se.Bytes())
				break
			case sql.NamedArg:
				err = errors.Warning("sql: predicate render failed").WithCause(fmt.Errorf("%s only can has named arg", p.Field))
				return
			case QueryExpr:
				sbb := bytebufferpool.Get()
				sub, subErr := se.Render(ctx, sbb)
				if subErr != nil {
					bytebufferpool.Put(sbb)
					err = errors.Warning("sql: predicate render failed").WithCause(subErr)
					return
				}
				exprs = append(exprs, sbb.Bytes())
				bytebufferpool.Put(sbb)
				argument = append(argument, sub...)
				break
			default:
				exprs = append(exprs, ctx.AcquireQueryPlaceholder())
				argument = append(argument, se)
				break
			}
		}

		_, _ = buf.Write(languages.LB)
		_, _ = buf.Write(bytes.Join(exprs, languages.COMMA))
		_, _ = buf.Write(languages.RB)
		break
	default:
		_, _ = buf.Write(ctx.AcquireQueryPlaceholder())
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
