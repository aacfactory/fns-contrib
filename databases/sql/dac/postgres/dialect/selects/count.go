package selects

import (
	"github.com/aacfactory/fns-contrib/databases/sql/dac/specifications"
	"github.com/valyala/bytebufferpool"
	"io"
)

func NewCountGeneric(ctx specifications.Context, spec *specifications.Specification) (generic *CountGeneric, err error) {
	buf := bytebufferpool.Get()
	defer bytebufferpool.Put(buf)
	// name
	tableName := ctx.FormatIdent([]byte(spec.Name))
	if spec.Schema != "" {
		schema := ctx.FormatIdent([]byte(spec.Schema))
		schema = append(schema, '.')
		tableName = append(schema, tableName...)
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
	_, _ = buf.Write([]byte("__COUNT__"))

	_, _ = buf.Write(specifications.SPACE)
	_, _ = buf.Write(specifications.FORM)
	_, _ = buf.Write(specifications.SPACE)
	_, _ = buf.Write(tableName)

	query := buf.Bytes()

	generic = &CountGeneric{
		spec:    spec,
		content: query,
		values:  nil,
	}

	return
}

type CountGeneric struct {
	spec    *specifications.Specification
	content []byte
	values  []int
}

func (generic *CountGeneric) Render(ctx specifications.Context, w io.Writer, cond specifications.Condition) (method specifications.Method, arguments []any, err error) {
	method = specifications.QueryMethod

	buf := bytebufferpool.Get()
	defer bytebufferpool.Put(buf)

	_, _ = buf.Write(generic.content)

	if cond.Exist() {
		_, _ = buf.Write(specifications.SPACE)
		_, _ = buf.Write(specifications.WHERE)
		_, _ = buf.Write(specifications.SPACE)
		arguments, err = cond.Render(ctx, buf)
		if err != nil {
			return
		}
	}

	query := buf.Bytes()

	_, err = w.Write(query)

	return
}
