package views

import (
	"github.com/aacfactory/errors"
	"github.com/aacfactory/fns-contrib/databases/postgres/dialect/selects/columns"
	"github.com/aacfactory/fns-contrib/databases/sql/dac/specifications"
	"github.com/valyala/bytebufferpool"
	"io"
	"strconv"
	"unsafe"
)

func NewViewGeneric(ctx specifications.Context, spec *specifications.Specification) (generic *ViewGeneric, err error) {
	buf := bytebufferpool.Get()
	defer bytebufferpool.Put(buf)
	var tableName []byte
	if spec.ViewBase == nil {
		tableName = ctx.FormatIdent([]byte(spec.Name))
		if spec.Schema != "" {
			schema := ctx.FormatIdent([]byte(spec.Schema))
			schema = append(schema, '.')
			tableName = append(schema, tableName...)
		}
	} else {
		tableName = ctx.FormatIdent([]byte(spec.ViewBase.Name))
		if spec.ViewBase.Schema != "" {
			schema := ctx.FormatIdent([]byte(spec.ViewBase.Schema))
			schema = append(schema, '.')
			tableName = append(schema, tableName...)
		}
	}
	// name

	_, _ = buf.Write(specifications.SELECT)
	_, _ = buf.Write(specifications.SPACE)

	fields := make([]string, 0, 1)
	for i, column := range spec.Columns {
		if i > 0 {
			_, _ = buf.Write(specifications.COMMA)
		}
		fragment, columnErr := columns.Fragment(ctx, spec, column)
		if columnErr != nil {
			err = errors.Warning("sql: new view generic failed").WithCause(columnErr).WithMeta("table", spec.Key)
			return
		}
		_, _ = buf.Write(fragment)
		fields = append(fields, column.Field)
	}

	_, _ = buf.Write(specifications.SPACE)
	_, _ = buf.Write(specifications.FORM)
	_, _ = buf.Write(specifications.SPACE)
	_, _ = buf.Write(tableName)

	query := buf.Bytes()

	generic = &ViewGeneric{
		spec:    spec,
		content: query,
		fields:  fields,
	}

	return
}

type ViewGeneric struct {
	spec    *specifications.Specification
	content []byte
	fields  []string
}

func (generic *ViewGeneric) Render(ctx specifications.Context, w io.Writer, cond specifications.Condition, orders specifications.Orders, groupBy specifications.GroupBy, offset int, length int) (method specifications.Method, arguments []any, fields []string, err error) {

	method = specifications.QueryMethod
	fields = generic.fields

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

	if len(orders) > 0 {
		_, _ = buf.Write(specifications.SPACE)
		_, orderErr := orders.Render(ctx, buf)
		if orderErr != nil {
			err = orderErr
			return
		}
	}

	if groupBy.Exist() {
		_, _ = buf.Write(specifications.SPACE)
		_, groupByErr := groupBy.Render(specifications.SwitchKey(ctx, generic.spec.Instance()), buf)
		if groupByErr != nil {
			err = groupByErr
			return
		}
	}

	if length > 0 {
		_, _ = buf.Write(specifications.SPACE)
		_, _ = buf.Write(specifications.OFFSET)
		_, _ = buf.Write(specifications.SPACE)
		os := strconv.Itoa(offset)
		_, _ = buf.Write(unsafe.Slice(unsafe.StringData(os), len(os)))
		_, _ = buf.Write(specifications.SPACE)
		_, _ = buf.Write(specifications.LIMIT)
		_, _ = buf.Write(specifications.SPACE)
		ls := strconv.Itoa(length)
		_, _ = buf.Write(unsafe.Slice(unsafe.StringData(ls), len(ls)))
	}

	query := buf.Bytes()

	_, err = w.Write(query)

	return
}