package selects

import (
	"fmt"
	"github.com/aacfactory/fns-contrib/databases/sql/dac/specifications"
	"github.com/aacfactory/fns/commons/bytex"
	"github.com/valyala/bytebufferpool"
	"io"
)

func NewExistGeneric(ctx specifications.Context, spec *specifications.Specification) (generic *ExistGeneric, err error) {
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
	_, _ = buf.WriteString("1")
	_, _ = buf.Write(specifications.SPACE)
	_, _ = buf.Write(specifications.AS)
	_, _ = buf.Write(specifications.SPACE)
	_, _ = buf.WriteString(ctx.FormatIdent("_EXIST_"))
	_, _ = buf.Write(specifications.SPACE)
	_, _ = buf.Write(specifications.FROM)
	_, _ = buf.Write(specifications.SPACE)
	_, _ = buf.WriteString(tableName)

	query := buf.String()

	generic = &ExistGeneric{
		spec:    spec,
		content: query,
	}
	return
}

type ExistGeneric struct {
	spec    *specifications.Specification
	content string
}

func (generic *ExistGeneric) Render(ctx specifications.Context, w io.Writer, cond specifications.Condition) (method specifications.Method, arguments []any, err error) {
	method = specifications.QueryMethod

	buf := bytebufferpool.Get()
	defer bytebufferpool.Put(buf)

	_, _ = buf.Write(bytex.FromString(generic.content))

	if cond.Exist() {
		_, _ = buf.Write(specifications.SPACE)
		_, _ = buf.Write(specifications.WHERE)
		_, _ = buf.Write(specifications.SPACE)
		arguments, err = cond.Render(ctx, buf)
		if err != nil {
			return
		}
	}

	query := buf.String()

	_, err = w.Write(bytex.FromString(query))

	return
}
