package updates

import (
	"fmt"
	"github.com/aacfactory/errors"
	"github.com/aacfactory/fns-contrib/databases/sql/dac/specifications"
	"github.com/aacfactory/fns/commons/bytex"
	"github.com/valyala/bytebufferpool"
	"io"
)

func NewUpdateGeneric(ctx specifications.Context, spec *specifications.Specification) (generic *UpdateGeneric, err error) {
	if spec.View {
		generic = &UpdateGeneric{}
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

	fields := make([]string, 0, 1)

	// pk
	pk, hasPk := spec.Pk()
	if !hasPk {
		err = errors.Warning("sql: new update generic failed").WithCause(fmt.Errorf("pk is required")).WithMeta("table", spec.Key)
		return
	}
	pkName := ctx.FormatIdent(pk.Name)
	// version
	ver, hasVer := spec.AuditVersion()
	verName := ""
	if hasVer {
		verName = ctx.FormatIdent(ver.Name)
	}

	n := 0
	_, _ = buf.Write(specifications.UPDATE)
	_, _ = buf.Write(specifications.SPACE)
	_, _ = buf.WriteString(tableName)
	_, _ = buf.Write(specifications.SPACE)
	_, _ = buf.Write(specifications.SET)
	_, _ = buf.Write(specifications.SPACE)

	if hasVer {
		_, _ = buf.WriteString(verName)
		_, _ = buf.Write(specifications.SPACE)
		_, _ = buf.Write(specifications.EQ)
		_, _ = buf.Write(specifications.SPACE)
		_, _ = buf.WriteString(verName)
		_, _ = buf.Write(specifications.PLUS)
		_, _ = buf.WriteString("1")
		n++
	}

	for _, column := range spec.Columns {
		skip := column.Kind == specifications.Pk || column.Kind == specifications.Aol ||
			column.Kind == specifications.Acb || column.Kind == specifications.Act ||
			column.Kind == specifications.Adb || column.Kind == specifications.Adt ||
			column.Kind == specifications.Virtual ||
			column.Kind == specifications.Link || column.Kind == specifications.Links
		if skip {
			continue
		}
		if n > 0 {
			_, _ = buf.Write(specifications.COMMA)
		}
		_, _ = buf.WriteString(ctx.FormatIdent(column.Name))
		_, _ = buf.Write(specifications.SPACE)
		_, _ = buf.Write(specifications.EQ)
		_, _ = buf.Write(specifications.SPACE)
		_, _ = buf.WriteString(ctx.NextQueryPlaceholder())
		fields = append(fields, column.Field)
		n++
	}

	_, _ = buf.Write(specifications.SPACE)
	_, _ = buf.Write(specifications.WHERE)
	_, _ = buf.Write(specifications.SPACE)
	// pk
	_, _ = buf.WriteString(pkName)
	_, _ = buf.Write(specifications.SPACE)
	_, _ = buf.Write(specifications.EQ)
	_, _ = buf.Write(specifications.SPACE)
	_, _ = buf.WriteString(ctx.NextQueryPlaceholder())
	fields = append(fields, pk.Field)
	if hasVer {
		_, _ = buf.Write(specifications.SPACE)
		_, _ = buf.Write(specifications.AND)
		_, _ = buf.Write(specifications.SPACE)
		_, _ = buf.WriteString(verName)
		_, _ = buf.Write(specifications.SPACE)
		_, _ = buf.Write(specifications.EQ)
		_, _ = buf.Write(specifications.SPACE)
		_, _ = buf.WriteString(ctx.NextQueryPlaceholder())
		fields = append(fields, ver.Field)
	}

	query := buf.String()

	generic = &UpdateGeneric{
		spec:    spec,
		content: query,
		fields:  fields,
	}

	return
}

type UpdateGeneric struct {
	spec    *specifications.Specification
	content string
	fields  []string
}

func (generic *UpdateGeneric) Render(_ specifications.Context, w io.Writer) (method specifications.Method, fields []string, err error) {
	method = specifications.ExecuteMethod
	fields = generic.fields

	_, err = w.Write(bytex.FromString(generic.content))
	if err != nil {
		return
	}

	return
}
