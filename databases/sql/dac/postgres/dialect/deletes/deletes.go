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
	indexes := make([]int, 0, 1)
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
	by, at, hasAD := spec.AuditDelete()
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
			indexes = append(indexes, by.FieldIdx)
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
			indexes = append(indexes, at.FieldIdx)
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
	indexes = append(indexes, pk.FieldIdx)
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
		indexes = append(indexes, ver.FieldIdx)
	}
	// where <<<

	query := buf.Bytes()

	generic = &DeleteGeneric{
		spec:    spec,
		content: query,
		values:  indexes,
	}

	return
}

type DeleteGeneric struct {
	spec    *specifications.Specification
	content []byte
	values  []int
}

func (generic *DeleteGeneric) Render(_ specifications.Context, w io.Writer, instance specifications.Table) (method specifications.Method, arguments []any, err error) {
	method = specifications.ExecuteMethod

	_, err = w.Write(generic.content)
	if err != nil {
		return
	}

	arguments, err = generic.spec.Arguments(instance, generic.values)
	if err != nil {
		return
	}

	return
}
