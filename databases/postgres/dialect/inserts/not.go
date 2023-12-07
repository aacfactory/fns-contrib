package inserts

import (
	"github.com/aacfactory/errors"
	"github.com/aacfactory/fns-contrib/databases/sql/dac/specifications"
	"github.com/valyala/bytebufferpool"
	"io"
	"strings"
)

func NewInsertWhenNotExistsGeneric(ctx specifications.Context, spec *specifications.Specification) (generic *InsertWhenNotExistsGeneric, err error) {
	if spec.View {
		generic = &InsertWhenNotExistsGeneric{}
		return
	}
	method, query, fields, returning, generateErr := generateInsertExistOrNotQuery(ctx, spec, false)
	if generateErr != nil {
		err = errors.Warning("sql: new insert when not exist generic failed").WithCause(generateErr).WithMeta("table", spec.Key)
		return
	}

	generic = &InsertWhenNotExistsGeneric{
		spec:      spec,
		method:    method,
		content:   query,
		fields:    fields,
		returning: returning,
	}
	return
}

type InsertWhenNotExistsGeneric struct {
	spec      *specifications.Specification
	method    specifications.Method
	content   string
	fields    []string
	returning []string
}

func (generic *InsertWhenNotExistsGeneric) Render(ctx specifications.Context, w io.Writer, src specifications.QueryExpr) (method specifications.Method, fields []string, arguments []any, returning []string, err error) {
	method = generic.method
	fields = generic.fields

	ctx.SkipNextQueryPlaceholderCursor(len(generic.fields))

	srcBuf := bytebufferpool.Get()
	defer bytebufferpool.Put(srcBuf)
	arguments, err = src.Render(ctx, srcBuf)
	if err != nil {
		return
	}
	srcQuery := srcBuf.String()

	query := strings.Replace(generic.content, srcPlaceHold, srcQuery, 1)
	_, err = w.Write([]byte(query))
	if err != nil {
		return
	}

	returning = generic.returning
	return
}
