package deletes

import (
	"github.com/aacfactory/fns-contrib/databases/sql/dac/specifications"
	"github.com/valyala/bytebufferpool"
	"io"
)

func NewDeleteByConditionsGeneric(ctx specifications.Context, spec *specifications.Specification) (generic *DeleteByConditionsGeneric, err error) {
	if spec.View {
		generic = &DeleteByConditionsGeneric{}
		return
	}
	buf := bytebufferpool.Get()
	defer bytebufferpool.Put(buf)
	// name
	tableName := ctx.FormatIdent([]byte(spec.Name))
	if spec.Schema != "" {
		schema := ctx.FormatIdent([]byte(spec.Schema))
		schema = append(schema, '.')
		tableName = append(schema, tableName...)
	}

	_, _ = buf.Write(specifications.DELETE)
	_, _ = buf.Write(specifications.SPACE)
	_, _ = buf.Write(specifications.FORM)
	_, _ = buf.Write(specifications.SPACE)
	_, _ = buf.Write(tableName)

	query := buf.Bytes()

	generic = &DeleteByConditionsGeneric{
		spec:    spec,
		content: query,
	}

	return
}

type DeleteByConditionsGeneric struct {
	spec    *specifications.Specification
	content []byte
}

func (generic *DeleteByConditionsGeneric) Render(ctx specifications.Context, w io.Writer, cond specifications.Condition) (method specifications.Method, arguments []any, err error) {
	method = specifications.ExecuteMethod

	_, err = w.Write(generic.content)
	if err != nil {
		return
	}

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
