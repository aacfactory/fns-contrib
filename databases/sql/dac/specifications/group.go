package specifications

import (
	"github.com/aacfactory/errors"
	"github.com/aacfactory/fns-contrib/databases/sql/dac/groups"
	"github.com/valyala/bytebufferpool"
	"io"
)

type GroupBy groups.Fields

func (group GroupBy) Render(ctx Context, w io.Writer) (argument []any, err error) {
	if len(group) == 0 {
		return
	}
	buf := bytebufferpool.Get()
	defer bytebufferpool.Put(buf)
	_, _ = buf.Write(GROUP)
	_, _ = buf.Write(SPACE)
	_, _ = buf.Write(BY)
	_, _ = buf.Write(SPACE)
	for i, field := range group {
		if i > 0 {
			_, _ = buf.Write(COMMA)
		}
		content, localed := ctx.Localization(field)
		if !localed {
			err = errors.Warning("sql: group by render failed").WithMeta("field", field)
			return
		}
		_, _ = buf.Write(content[0])
	}
	_, err = w.Write(buf.Bytes())
	if err != nil {
		err = errors.Warning("sql: group by render failed").WithCause(err)
		return
	}
	return
}

type Having struct {
	groups.HavingCondition
}

func (having Having) Render(ctx Context, w io.Writer) (argument []any, err error) {
	if !having.Cond.Exist() {
		return
	}
	buf := bytebufferpool.Get()
	defer bytebufferpool.Put(buf)
	_, _ = buf.Write(HAVING)
	_, _ = buf.Write(SPACE)
	argument, err = Condition{having.Cond}.Render(ctx, buf)
	if err != nil {
		err = errors.Warning("sql: having render failed").WithCause(err)
		return
	}
	_, err = w.Write(buf.Bytes())
	if err != nil {
		err = errors.Warning("sql: having render failed").WithCause(err)
		return
	}
	return
}
