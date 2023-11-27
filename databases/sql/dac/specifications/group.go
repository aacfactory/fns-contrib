package specifications

import (
	"github.com/aacfactory/errors"
	"github.com/aacfactory/fns-contrib/databases/sql/dac/conditions"
	"github.com/valyala/bytebufferpool"
	"io"
)

type GroupBy []string

func (group GroupBy) Render(ctx Context, w io.Writer) (argument []any, err error) {
	buf := bytebufferpool.Get()
	defer bytebufferpool.Put(buf)
	for _, field := range group {
		content, localed := ctx.Localization(field)
		if !localed {
			err = errors.Warning("sql: group by render failed").WithMeta("field", field)
			return
		}
		_, _ = buf.Write(COMMA)
		_, _ = buf.Write(content[0])
	}
	p := buf.Bytes()
	if len(p) > 0 {
		_, err = w.Write(p[2:])
		if err != nil {
			err = errors.Warning("sql: group by render failed").WithCause(err)
			return
		}
	}
	return
}

func NewGroupBy(field ...string) GroupBy {
	return field
}

type Having struct {
	Cond conditions.Condition
}

func (having Having) Render(ctx Context, w io.Writer) (argument []any, err error) {
	argument, err = Condition{having.Cond}.Render(ctx, w)
	if err != nil {
		err = errors.Warning("sql: having render failed").WithCause(err)
		return
	}
	return
}

func NewHaving(cond conditions.Condition) Having {
	return Having{
		Cond: cond,
	}
}
