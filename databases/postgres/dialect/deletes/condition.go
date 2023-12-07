package deletes

import (
	"fmt"
	"github.com/aacfactory/fns-contrib/databases/sql/dac/specifications"
	"github.com/aacfactory/fns/commons/bytex"
	"github.com/valyala/bytebufferpool"
	"io"
)

func NewDeleteByConditionsGeneric(ctx specifications.Context, spec *specifications.Specification) (generic *DeleteByConditionsGeneric, err error) {
	if spec.View {
		generic = &DeleteByConditionsGeneric{}
		return
	}
	var audits []string
	buf := bytebufferpool.Get()
	defer bytebufferpool.Put(buf)
	// name
	tableName := ctx.FormatIdent(spec.Name)
	if spec.Schema != "" {
		schema := ctx.FormatIdent(spec.Schema)
		tableName = fmt.Sprintf("%s.%s", schema, tableName)
	}

	by, at, hasAd := spec.AuditDeletion()
	if hasAd {
		n := 0
		_, _ = buf.Write(specifications.UPDATE)
		_, _ = buf.Write(specifications.SPACE)
		_, _ = buf.WriteString(tableName)
		_, _ = buf.Write(specifications.SPACE)
		_, _ = buf.Write(specifications.SET)
		ver, hasVer := spec.AuditVersion()
		if hasVer {
			verName := ctx.FormatIdent(ver.Name)
			_, _ = buf.Write(specifications.SPACE)
			_, _ = buf.WriteString(verName)
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
			_, _ = buf.WriteString(ctx.FormatIdent(by.Name))
			_, _ = buf.Write(specifications.SPACE)
			_, _ = buf.Write(specifications.EQ)
			_, _ = buf.Write(specifications.SPACE)
			_, _ = buf.WriteString(ctx.NextQueryPlaceholder())
			audits = append(audits, by.Field)
			n++
		}
		if at != nil {
			if n > 0 {
				_, _ = buf.Write(specifications.COMMA)
			}
			_, _ = buf.WriteString(ctx.FormatIdent(at.Name))
			_, _ = buf.Write(specifications.SPACE)
			_, _ = buf.Write(specifications.EQ)
			_, _ = buf.Write(specifications.SPACE)
			_, _ = buf.WriteString(ctx.NextQueryPlaceholder())
			audits = append(audits, at.Field)
			n++
		}

	} else {
		_, _ = buf.Write(specifications.DELETE)
		_, _ = buf.Write(specifications.SPACE)
		_, _ = buf.Write(specifications.FROM)
		_, _ = buf.Write(specifications.SPACE)
		_, _ = buf.WriteString(tableName)
	}

	query := buf.String()

	generic = &DeleteByConditionsGeneric{
		spec:    spec,
		content: query,
		audits:  audits,
	}

	return
}

type DeleteByConditionsGeneric struct {
	spec    *specifications.Specification
	content string
	audits  []string
}

func (generic *DeleteByConditionsGeneric) Render(ctx specifications.Context, w io.Writer, cond specifications.Condition) (method specifications.Method, audits []string, arguments []any, err error) {
	method = specifications.ExecuteMethod
	audits = generic.audits

	_, err = w.Write(bytex.FromString(generic.content))
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
