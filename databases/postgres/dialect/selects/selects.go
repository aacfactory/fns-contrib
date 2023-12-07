package selects

import (
	"fmt"
	"github.com/aacfactory/errors"
	"github.com/aacfactory/fns-contrib/databases/postgres/dialect/selects/columns"
	"github.com/aacfactory/fns-contrib/databases/sql/dac/specifications"
	"github.com/valyala/bytebufferpool"
	"io"
)

func NewQueryGeneric(ctx specifications.Context, spec *specifications.Specification) (generic *QueryGeneric, err error) {
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

	fields := make([]string, 0, 1)
	for i, column := range spec.Columns {
		if i > 0 {
			_, _ = buf.Write(specifications.COMMA)
		}
		fragment, columnErr := columns.Fragment(ctx, spec, column)
		if columnErr != nil {
			err = errors.Warning("sql: new query generic failed").WithCause(columnErr).WithMeta("table", spec.Key)
			return
		}
		_, _ = buf.WriteString(fragment)
		fields = append(fields, column.Field)
	}

	_, _ = buf.Write(specifications.SPACE)
	_, _ = buf.Write(specifications.FROM)
	_, _ = buf.Write(specifications.SPACE)
	_, _ = buf.WriteString(tableName)

	query := buf.String()

	generic = &QueryGeneric{
		spec:    spec,
		content: query,
		fields:  fields,
	}

	return
}

type QueryGeneric struct {
	spec    *specifications.Specification
	content string
	fields  []string
}

func (generic *QueryGeneric) Render(ctx specifications.Context, w io.Writer, cond specifications.Condition, orders specifications.Orders, offset int, length int) (method specifications.Method, arguments []any, fields []string, err error) {
	method = specifications.QueryMethod
	fields = generic.fields

	buf := bytebufferpool.Get()
	defer bytebufferpool.Put(buf)

	_, _ = buf.WriteString(generic.content)

	if cond.Exist() {
		_, _ = buf.Write(specifications.SPACE)
		_, _ = buf.Write(specifications.WHERE)
		_, _ = buf.Write(specifications.SPACE)
		arguments, err = cond.Render(ctx, buf)
		if err != nil {
			return
		}
	}

	if len(orders) > 0 {
		_, _ = buf.Write(specifications.SPACE)
		_, orderErr := orders.Render(ctx, buf)
		if orderErr != nil {
			err = orderErr
			return
		}
	}

	if length > 0 {
		_, _ = buf.Write(specifications.SPACE)
		_, _ = buf.Write(specifications.OFFSET)
		_, _ = buf.Write(specifications.SPACE)
		_, _ = buf.WriteString(ctx.NextQueryPlaceholder())
		_, _ = buf.Write(specifications.SPACE)
		_, _ = buf.Write(specifications.LIMIT)
		_, _ = buf.Write(specifications.SPACE)
		_, _ = buf.WriteString(ctx.NextQueryPlaceholder())
	}

	query := buf.String()

	_, err = w.Write([]byte(query))

	return
}
