package selects

import (
	"fmt"
	"github.com/aacfactory/fns-contrib/databases/sql/dac/specifications"
	"github.com/valyala/bytebufferpool"
	"io"
)

func NewCountGeneric(ctx specifications.Context, spec *specifications.Specification) (generic *CountGeneric, err error) {
	buf := bytebufferpool.Get()
	defer bytebufferpool.Put(buf)
	// name
	tableName := ctx.FormatIdent(spec.Name)
	if spec.Schema != "" {
		schema := ctx.FormatIdent(spec.Schema)
		tableName = fmt.Sprintf("%s.%s", schema, tableName)
	}

	_, _ = buf.Write(specifications.SELECT)
	_, _ = buf.Write(specifications.SPACE)

	_, _ = buf.Write(specifications.COUNT)
	_, _ = buf.Write(specifications.LB)
	_, _ = buf.Write([]byte("1"))
	_, _ = buf.Write(specifications.RB)
	_, _ = buf.Write(specifications.SPACE)
	_, _ = buf.Write(specifications.AS)
	_, _ = buf.Write(specifications.SPACE)
	_, _ = buf.WriteString(ctx.FormatIdent("_COUNT_"))

	_, _ = buf.Write(specifications.SPACE)
	_, _ = buf.Write(specifications.FROM)
	_, _ = buf.Write(specifications.SPACE)
	_, _ = buf.WriteString(tableName)

	query := []byte(buf.String())

	generic = &CountGeneric{
		spec:    spec,
		content: query,
	}

	return
}

type CountGeneric struct {
	spec    *specifications.Specification
	content []byte
}

func (generic *CountGeneric) Render(ctx specifications.Context, w io.Writer, cond specifications.Condition) (method specifications.Method, arguments []any, err error) {
	method = specifications.QueryMethod

	_, _ = w.Write(generic.content)

	if cond.Exist() {
		_, _ = w.Write(specifications.SPACE)
		_, _ = w.Write(specifications.WHERE)
		_, _ = w.Write(specifications.SPACE)
		arguments, err = cond.Render(ctx, w)
		if err != nil {
			return
		}
	}

	return
}
