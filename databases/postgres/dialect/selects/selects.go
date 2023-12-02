package selects

import (
	"github.com/aacfactory/errors"
	"github.com/aacfactory/fns-contrib/databases/postgres/dialect/selects/columns"
	"github.com/aacfactory/fns-contrib/databases/sql/dac/specifications"
	"github.com/valyala/bytebufferpool"
	"io"
	"strconv"
	"unsafe"
)

func NewQueryGeneric(ctx specifications.Context, spec *specifications.Specification) (generic *QueryGeneric, err error) {
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

	_, _ = buf.Write(specifications.LB)

	fields := make([]int, 0, 1)
	for i, column := range spec.Columns {
		if i > 0 {
			_, _ = buf.Write(specifications.COMMA)
		}
		fragment, columnErr := columns.Fragment(ctx, spec, column)
		if columnErr != nil {
			err = errors.Warning("sql: new query generic failed").WithCause(columnErr).WithMeta("table", spec.Key)
			return
		}
		_, _ = buf.Write(fragment)
		fields = append(fields, i)
	}
	_, _ = buf.Write(specifications.RB)

	_, _ = buf.Write(specifications.SPACE)
	_, _ = buf.Write(specifications.FORM)
	_, _ = buf.Write(tableName)

	query := buf.Bytes()

	generic = &QueryGeneric{
		spec:    spec,
		content: query,
		columns: fields,
	}

	return
}

type QueryGeneric struct {
	spec    *specifications.Specification
	content []byte
	columns []int
}

func (generic *QueryGeneric) Render(ctx specifications.Context, w io.Writer, cond specifications.Condition, orders specifications.Orders, groupBy specifications.GroupBy, having specifications.Having, offset int, length int) (method specifications.Method, arguments []any, columns []int, err error) {
	method = specifications.QueryMethod

	buf := bytebufferpool.Get()
	defer bytebufferpool.Put(buf)

	_, _ = buf.Write(generic.content)

	if cond.Exist() {
		_, _ = buf.Write(specifications.SPACE)
		_, _ = buf.Write(specifications.WHERE)
		_, _ = buf.Write(specifications.SPACE)
		arguments, err = cond.Render(ctx, w)
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
	if len(groupBy) > 0 {
		_, _ = buf.Write(specifications.SPACE)
		_, groupByErr := groupBy.Render(ctx, buf)
		if groupByErr != nil {
			err = groupByErr
			return
		}
	}
	if having.Cond.Exist() {
		_, _ = buf.Write(specifications.SPACE)
		_, havingErr := having.Render(ctx, buf)
		if havingErr != nil {
			err = havingErr
			return
		}
	}
	if length > 0 {
		_, _ = buf.Write(specifications.SPACE)
		_, _ = buf.Write(specifications.OFFSET)
		_, _ = buf.Write(specifications.SPACE)
		ls := strconv.Itoa(length)
		_, _ = buf.Write(unsafe.Slice(unsafe.StringData(ls), len(ls)))
		_, _ = buf.Write(specifications.SPACE)
		_, _ = buf.Write(specifications.LIMIT)
		_, _ = buf.Write(specifications.OFFSET)
		os := strconv.Itoa(offset)
		_, _ = buf.Write(unsafe.Slice(unsafe.StringData(os), len(os)))
	}

	query := buf.Bytes()

	_, err = w.Write(query)

	columns = generic.columns

	return
}
