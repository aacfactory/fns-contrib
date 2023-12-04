package updates

import (
	"fmt"
	"github.com/aacfactory/errors"
	"github.com/aacfactory/fns-contrib/databases/sql/dac/specifications"
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
	tableName := ctx.FormatIdent([]byte(spec.Name))
	if spec.Schema != "" {
		schema := ctx.FormatIdent([]byte(spec.Schema))
		schema = append(schema, '.')
		tableName = append(schema, tableName...)
	}

	fields := make([]string, 0, 1)

	// pk
	pk, hasPk := spec.Pk()
	if !hasPk {
		err = errors.Warning("sql: new update generic failed").WithCause(fmt.Errorf("pk is required")).WithMeta("table", spec.Key)
		return
	}
	pkName := ctx.FormatIdent([]byte(pk.Name))
	// version
	ver, hasVer := spec.AuditVersion()
	var verName []byte
	if hasVer {
		verName = ctx.FormatIdent([]byte(ver.Name))
	}

	n := 0
	_, _ = buf.Write(specifications.UPDATE)
	_, _ = buf.Write(specifications.SPACE)
	_, _ = buf.Write(tableName)
	_, _ = buf.Write(specifications.SPACE)
	_, _ = buf.Write(specifications.SET)
	_, _ = buf.Write(specifications.SPACE)

	if hasVer {
		_, _ = buf.Write(verName)
		_, _ = buf.Write(specifications.SPACE)
		_, _ = buf.Write(specifications.EQ)
		_, _ = buf.Write(specifications.SPACE)
		_, _ = buf.Write(verName)
		_, _ = buf.Write(specifications.PLUS)
		_, _ = buf.Write([]byte("1"))
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
		_, _ = buf.Write(ctx.FormatIdent([]byte(column.Name)))
		_, _ = buf.Write(specifications.SPACE)
		_, _ = buf.Write(specifications.EQ)
		_, _ = buf.Write(specifications.SPACE)
		_, _ = buf.Write(ctx.NextQueryPlaceholder())
		fields = append(fields, column.Field)
		n++
	}

	_, _ = buf.Write(specifications.SPACE)
	_, _ = buf.Write(specifications.WHERE)
	_, _ = buf.Write(specifications.SPACE)
	// pk
	_, _ = buf.Write(pkName)
	_, _ = buf.Write(specifications.SPACE)
	_, _ = buf.Write(specifications.EQ)
	_, _ = buf.Write(specifications.SPACE)
	_, _ = buf.Write(ctx.NextQueryPlaceholder())
	fields = append(fields, pk.Field)
	if hasVer {
		_, _ = buf.Write(specifications.SPACE)
		_, _ = buf.Write(specifications.AND)
		_, _ = buf.Write(specifications.SPACE)
		_, _ = buf.Write(verName)
		_, _ = buf.Write(specifications.SPACE)
		_, _ = buf.Write(specifications.EQ)
		_, _ = buf.Write(specifications.SPACE)
		_, _ = buf.Write(ctx.NextQueryPlaceholder())
		fields = append(fields, ver.Field)
	}

	query := []byte(buf.String())

	generic = &UpdateGeneric{
		spec:    spec,
		content: query,
		fields:  fields,
	}

	return
}

type UpdateGeneric struct {
	spec    *specifications.Specification
	content []byte
	fields  []string
}

func (generic *UpdateGeneric) Render(_ specifications.Context, w io.Writer) (method specifications.Method, fields []string, err error) {
	method = specifications.ExecuteMethod
	fields = generic.fields

	_, err = w.Write(generic.content)
	if err != nil {
		return
	}

	return
}
