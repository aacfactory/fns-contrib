package specifications

import (
	"github.com/aacfactory/errors"
	"github.com/aacfactory/fns-contrib/databases/sql/dac/groups"
	"github.com/valyala/bytebufferpool"
	"io"
)

type GroupBy struct {
	groups.GroupBy
}

func (group GroupBy) Exist() bool {
	return len(group.Bys) > 0
}

func (group GroupBy) Render(ctx Context, w io.Writer) (argument []any, err error) {
	buf := bytebufferpool.Get()
	defer bytebufferpool.Put(buf)
	_, _ = buf.Write(GROUP)
	_, _ = buf.Write(SPACE)
	_, _ = buf.Write(BY)
	_, _ = buf.Write(SPACE)
	for i, field := range group.Bys {
		if i > 0 {
			_, _ = buf.Write(COMMA)
		}
		content, localed := ctx.Localization(field)
		if !localed {
			err = errors.Warning("sql: group by render failed").WithMeta("field", field)
			return
		}
		_, _ = buf.WriteString(content[0])
	}
	if group.Havings.Exist() {
		_, _ = buf.Write(SPACE)
		_, _ = buf.Write(HAVING)
		_, _ = buf.Write(SPACE)
		argument, err = Condition{group.Havings}.Render(ctx, buf)
		if err != nil {
			err = errors.Warning("sql: group by render failed").WithCause(err)
			return
		}
	}
	_, err = w.Write([]byte(buf.String()))
	if err != nil {
		err = errors.Warning("sql: group by render failed").WithCause(err)
		return
	}
	return
}
