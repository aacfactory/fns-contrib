package deletes

import (
	"github.com/aacfactory/fns-contrib/databases/sql/dac/specifications"
	"github.com/valyala/bytebufferpool"
	"io"
)

func NewDeleteByConditionsGeneric(ctx specifications.Context, spec *specifications.Specification) (generic *DeleteByConditionsGeneric, err error) {
	if spec.View {
		generic = &DeleteByConditionsGeneric{}
		return
	}
	var audits []int
	buf := bytebufferpool.Get()
	defer bytebufferpool.Put(buf)
	// name
	tableName := ctx.FormatIdent([]byte(spec.Name))
	if spec.Schema != "" {
		schema := ctx.FormatIdent([]byte(spec.Schema))
		schema = append(schema, '.')
		tableName = append(schema, tableName...)
	}

	by, at, hasAd := spec.AuditDeletion()
	if hasAd {
		n := 0
		_, _ = buf.Write(specifications.UPDATE)
		_, _ = buf.Write(specifications.SPACE)
		_, _ = buf.Write(tableName)
		_, _ = buf.Write(specifications.SPACE)
		_, _ = buf.Write(specifications.SET)
		ver, hasVer := spec.AuditVersion()
		if hasVer {
			verName := ctx.FormatIdent([]byte(ver.Name))
			_, _ = buf.Write(specifications.SPACE)
			_, _ = buf.Write(verName)
			_, _ = buf.Write(specifications.SPACE)
			_, _ = buf.Write(specifications.EQ)
			_, _ = buf.Write(specifications.SPACE)
			_, _ = buf.Write(specifications.PLUS)
			_, _ = buf.Write([]byte("1"))
			n++
		}
		if by != nil {
			if n > 0 {
				_, _ = buf.Write(specifications.COMMA)
			}
			_, _ = buf.Write(ctx.FormatIdent([]byte(by.Name)))
			_, _ = buf.Write(specifications.SPACE)
			_, _ = buf.Write(specifications.EQ)
			_, _ = buf.Write(specifications.SPACE)
			_, _ = buf.Write(ctx.NextQueryPlaceholder())
			audits = append(audits, by.FieldIdx)
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
			audits = append(audits, at.FieldIdx)
			n++
		}

	} else {
		_, _ = buf.Write(specifications.DELETE)
		_, _ = buf.Write(specifications.SPACE)
		_, _ = buf.Write(specifications.FORM)
		_, _ = buf.Write(specifications.SPACE)
		_, _ = buf.Write(tableName)
	}

	query := buf.Bytes()

	generic = &DeleteByConditionsGeneric{
		spec:    spec,
		content: query,
	}

	return
}

type DeleteByConditionsGeneric struct {
	spec    *specifications.Specification
	content []byte
	audits  []int
}

func (generic *DeleteByConditionsGeneric) Render(ctx specifications.Context, w io.Writer, cond specifications.Condition) (method specifications.Method, audits []int, arguments []any, err error) {
	method = specifications.ExecuteMethod
	audits = generic.audits

	_, err = w.Write(generic.content)
	if err != nil {
		return
	}

	if cond.Exist() {
		_, _ = w.Write(specifications.SPACE)
		_, _ = w.Write(specifications.WHERE)
		_, _ = w.Write(specifications.SPACE)

		ctx.SkipNextQueryPlaceholderCursor(len(audits))
		arguments, err = cond.Render(ctx, w)
		if err != nil {
			return
		}
	}

	return
}
