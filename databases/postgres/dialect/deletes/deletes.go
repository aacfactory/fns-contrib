package deletes

import (
	"fmt"
	"github.com/aacfactory/errors"
	"github.com/aacfactory/fns-contrib/databases/sql/dac/specifications"
	"github.com/valyala/bytebufferpool"
	"io"
)

func NewDeleteGeneric(ctx specifications.Context, spec *specifications.Specification) (generic *DeleteGeneric, err error) {
	if spec.View {
		generic = &DeleteGeneric{}
		return
	}
	buf := bytebufferpool.Get()
	defer bytebufferpool.Put(buf)
	fields := make([]string, 0, 1)
	// name
	tableName := ctx.FormatIdent([]byte(spec.Name))
	if spec.Schema != "" {
		schema := ctx.FormatIdent([]byte(spec.Schema))
		schema = append(schema, '.')
		tableName = append(schema, tableName...)
	}
	// pk
	pk, hasPk := spec.Pk()
	if !hasPk {
		err = errors.Warning("sql: new delete generic failed").WithCause(fmt.Errorf("pk is required")).WithMeta("table", spec.Key)
		return
	}
	pkName := ctx.FormatIdent([]byte(pk.Name))
	// version
	ver, hasVer := spec.AuditVersion()
	var verName []byte
	if hasVer {
		verName = ctx.FormatIdent([]byte(ver.Name))
	}
	// adb adt
	by, at, hasAD := spec.AuditDeletion()
	if hasAD {
		n := 0
		_, _ = buf.Write(specifications.UPDATE)
		_, _ = buf.Write(specifications.SPACE)
		_, _ = buf.Write(tableName)
		_, _ = buf.Write(specifications.SPACE)
		_, _ = buf.Write(specifications.SET)
		_, _ = buf.Write(specifications.SPACE)
		if by != nil {
			_, _ = buf.Write(ctx.FormatIdent([]byte(by.Name)))
			_, _ = buf.Write(specifications.SPACE)
			_, _ = buf.Write(specifications.EQ)
			_, _ = buf.Write(specifications.SPACE)
			_, _ = buf.Write(ctx.NextQueryPlaceholder())
			fields = append(fields, by.Field)
			n++
		}
		if at != nil {
			if n > 0 {
				_, _ = buf.Write(specifications.COMMA)
			}
			_, _ = buf.Write(ctx.FormatIdent([]byte(at.Name)))
			_, _ = buf.Write(specifications.SPACE)
			_, _ = buf.Write(specifications.EQ)
			_, _ = buf.Write(specifications.SPACE)
			_, _ = buf.Write(ctx.NextQueryPlaceholder())
			fields = append(fields, at.Field)
			n++
		}
		// version
		if hasVer {
			if n > 0 {
				_, _ = buf.Write(specifications.COMMA)
			}
			_, _ = buf.Write(verName)
			_, _ = buf.Write(specifications.SPACE)
			_, _ = buf.Write(specifications.EQ)
			_, _ = buf.Write(specifications.SPACE)
			_, _ = buf.Write(verName)
			_, _ = buf.Write(specifications.PLUS)
			_, _ = buf.Write([]byte("1"))
			n++
		}
		_, _ = buf.Write(specifications.SPACE)
	} else {
		_, _ = buf.Write(specifications.DELETE)
		_, _ = buf.Write(specifications.SPACE)
		_, _ = buf.Write(specifications.FORM)
		_, _ = buf.Write(specifications.SPACE)
		_, _ = buf.Write(tableName)
		_, _ = buf.Write(specifications.SPACE)
	}

	// where >>>
	_, _ = buf.Write(specifications.WHERE)
	_, _ = buf.Write(specifications.SPACE)
	// pk
	_, _ = buf.Write(pkName)
	_, _ = buf.Write(specifications.SPACE)
	_, _ = buf.Write(specifications.EQ)
	_, _ = buf.Write(specifications.SPACE)
	_, _ = buf.Write(ctx.NextQueryPlaceholder())
	fields = append(fields, pk.Field)
	// version
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
	// where <<<

	query := []byte(buf.String())

	generic = &DeleteGeneric{
		spec:    spec,
		content: query,
		fields:  fields,
	}

	return
}

type DeleteGeneric struct {
	spec    *specifications.Specification
	content []byte
	fields  []string
}

func (generic *DeleteGeneric) Render(_ specifications.Context, w io.Writer) (method specifications.Method, fields []string, err error) {
	method = specifications.ExecuteMethod
	fields = generic.fields

	_, err = w.Write(generic.content)
	if err != nil {
		return
	}

	return
}
