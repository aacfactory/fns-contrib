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
	tableName := ctx.FormatIdent(spec.Name)
	if spec.Schema != "" {
		schema := ctx.FormatIdent(spec.Schema)
		tableName = fmt.Sprintf("%s.%s", schema, tableName)
	}

	ver, hasVer := spec.AuditVersion()
	verName := ""
	if hasVer {
		verName = ctx.FormatIdent(ver.Name)
	}

	generic = &UpdateFieldsGeneric{
		spec:    spec,
		table:   []byte(tableName),
		version: []byte(verName),
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

	_, _ = w.Write(specifications.UPDATE)
	_, _ = w.Write(specifications.SPACE)
	_, _ = w.Write(generic.table)
	_, _ = w.Write(specifications.SPACE)
	_, _ = w.Write(specifications.SET)
	_, _ = w.Write(specifications.SPACE)

	n := 0
	if len(generic.version) > 0 {
		_, _ = w.Write(generic.version)
		_, _ = w.Write(specifications.SPACE)
		_, _ = w.Write(specifications.EQ)
		_, _ = w.Write(specifications.SPACE)
		_, _ = w.Write(generic.version)
		_, _ = w.Write(specifications.PLUS)
		_, _ = w.Write([]byte("1"))
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
			_, _ = w.Write(specifications.COMMA)
		}
		_, _ = w.Write([]byte(ctx.FormatIdent(column.Name)))
		_, _ = w.Write(specifications.SPACE)
		_, _ = w.Write(specifications.EQ)
		_, _ = w.Write(specifications.SPACE)
		_, _ = w.Write([]byte(ctx.NextQueryPlaceholder()))
		arguments = append(arguments, field.Value)
		n++
	}

	if cond.Exist() {
		_, _ = w.Write(specifications.SPACE)
		_, _ = w.Write(specifications.WHERE)
		_, _ = w.Write(specifications.SPACE)
		condValues, condErr := cond.Render(ctx, w)
		if condErr != nil {
			err = errors.Warning("sql: render update field failed").WithCause(condErr)
			return
		}
		arguments = append(arguments, condValues...)
	}

	return
}
