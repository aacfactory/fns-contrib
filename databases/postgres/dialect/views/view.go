package views

import (
	"fmt"
	"github.com/aacfactory/errors"
	"github.com/aacfactory/fns-contrib/databases/postgres/dialect/selects/columns"
	"github.com/aacfactory/fns-contrib/databases/sql/dac/specifications"
	"github.com/aacfactory/fns/commons/bytex"
	"github.com/valyala/bytebufferpool"
	"io"
)

func NewViewGeneric(ctx specifications.Context, spec *specifications.Specification) (generic *ViewGeneric, err error) {
	buf := bytebufferpool.Get()
	defer bytebufferpool.Put(buf)
	tableName := ""
	if spec.ViewBase == nil {
		tableName = ctx.FormatIdent(spec.Name)
		if spec.Schema != "" {
			schema := ctx.FormatIdent(spec.Schema)
			tableName = fmt.Sprintf("%s.%s", schema, tableName)
		}
	} else {
		tableName = ctx.FormatIdent(spec.ViewBase.Name)
		if spec.ViewBase.Schema != "" {
			schema := ctx.FormatIdent(spec.ViewBase.Schema)
			tableName = fmt.Sprintf("%s.%s", schema, tableName)
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
		_, _ = buf.WriteString(fragment)
		fields = append(fields, column.Field)
	}

	_, _ = buf.Write(specifications.SPACE)
	_, _ = buf.Write(specifications.FROM)
	_, _ = buf.Write(specifications.SPACE)
	_, _ = buf.WriteString(tableName)

	query := buf.String()

	generic = &ViewGeneric{
		spec:    spec,
		content: []byte(query),
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

	if len(orders) > 0 {
		_, _ = w.Write(specifications.SPACE)
		_, orderErr := orders.Render(ctx, w)
		if orderErr != nil {
			err = orderErr
			return
		}
	}

	if groupBy.Exist() {
		_, _ = w.Write(specifications.SPACE)
		_, groupByErr := groupBy.Render(specifications.SwitchKey(ctx, generic.spec.Instance()), w)
		if groupByErr != nil {
			err = groupByErr
			return
		}
	}

	if length > 0 {
		_, _ = w.Write(specifications.SPACE)
		_, _ = w.Write(specifications.OFFSET)
		_, _ = w.Write(specifications.SPACE)
		_, _ = w.Write(bytex.FromString(ctx.NextQueryPlaceholder()))
		_, _ = w.Write(specifications.SPACE)
		_, _ = w.Write(specifications.LIMIT)
		_, _ = w.Write(specifications.SPACE)
		_, _ = w.Write(bytex.FromString(ctx.NextQueryPlaceholder()))
	}

	return
}
