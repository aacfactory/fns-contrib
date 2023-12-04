package updates

import (
	"fmt"
	"github.com/aacfactory/errors"
	"github.com/aacfactory/fns-contrib/databases/sql/dac/specifications"
	"github.com/valyala/bytebufferpool"
	"io"
)

func NewUpdateFieldsGeneric(ctx specifications.Context, spec *specifications.Specification) (generic *UpdateFieldsGeneric, err error) {
	if spec.View {
		generic = &UpdateFieldsGeneric{}
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

	ver, hasVer := spec.AuditVersion()
	var verName []byte
	if hasVer {
		verName = ctx.FormatIdent([]byte(ver.Name))
	}

	generic = &UpdateFieldsGeneric{
		spec:    spec,
		table:   tableName,
		version: verName,
	}

	return
}

type UpdateFieldsGeneric struct {
	spec    *specifications.Specification
	table   []byte
	version []byte
}

func (generic *UpdateFieldsGeneric) Render(ctx specifications.Context, w io.Writer, fields []specifications.FieldValue, cond specifications.Condition) (method specifications.Method, arguments []any, err error) {
	if len(fields) == 0 {
		err = errors.Warning("sql: render update field failed").WithCause(fmt.Errorf("fields is required"))
		return
	}

	method = specifications.ExecuteMethod

	buf := bytebufferpool.Get()
	defer bytebufferpool.Put(buf)

	_, _ = buf.Write(specifications.UPDATE)
	_, _ = buf.Write(specifications.SPACE)
	_, _ = buf.Write(generic.table)
	_, _ = buf.Write(specifications.SPACE)
	_, _ = buf.Write(specifications.SET)
	_, _ = buf.Write(specifications.SPACE)

	n := 0
	if len(generic.version) > 0 {
		_, _ = buf.Write(generic.table)
		_, _ = buf.Write(specifications.SPACE)
		_, _ = buf.Write(specifications.EQ)
		_, _ = buf.Write(specifications.SPACE)
		_, _ = buf.Write(generic.table)
		_, _ = buf.Write(specifications.PLUS)
		_, _ = buf.Write([]byte("1"))
		n++
	}

	for _, field := range fields {
		column, hasColumn := generic.spec.ColumnByField(field.Name)
		if !hasColumn {
			err = errors.Warning("sql: render update field failed").WithCause(fmt.Errorf("%s field was not found in %s", field.Name, generic.spec.Key))
			return
		}
		valid := column.Kind == specifications.Normal ||
			column.Kind == specifications.Amb || column.Kind == specifications.Amt ||
			column.Kind == specifications.Reference ||
			column.Kind == specifications.Json
		if !valid {
			err = errors.Warning("sql: render update field failed").WithCause(fmt.Errorf("%s field in %s cant be modified", field.Name, generic.spec.Key))
			return
		}
		if n > 0 {
			_, _ = buf.Write(specifications.COMMA)
		}
		_, _ = buf.Write(ctx.FormatIdent([]byte(column.Name)))
		_, _ = buf.Write(specifications.SPACE)
		_, _ = buf.Write(specifications.EQ)
		_, _ = buf.Write(specifications.SPACE)
		_, _ = buf.Write(ctx.NextQueryPlaceholder())
		arguments = append(arguments, field.Value)
		n++
	}

	if cond.Exist() {
		_, _ = buf.Write(specifications.SPACE)
		_, _ = buf.Write(specifications.WHERE)
		_, _ = buf.Write(specifications.SPACE)
		condValues, condErr := cond.Render(ctx, buf)
		if condErr != nil {
			err = errors.Warning("sql: render update field failed").WithCause(condErr)
			return
		}
		arguments = append(arguments, condValues...)
	}

	query := []byte(buf.String())
	_, err = w.Write(query)
	if err != nil {
		err = errors.Warning("sql: render update field failed").WithCause(err)
		return
	}

	return
}
