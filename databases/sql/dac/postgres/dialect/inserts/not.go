package inserts

import (
	"bytes"
	"github.com/aacfactory/errors"
	"github.com/aacfactory/fns-contrib/databases/sql/dac/specifications"
	"github.com/valyala/bytebufferpool"
	"io"
)

func NewInsertWhenNotExistsGeneric(ctx specifications.Context, spec *specifications.Specification) (generic *InsertWhenNotExistsGeneric, err error) {
	if spec.View {
		generic = &InsertWhenNotExistsGeneric{}
		return
	}
	method, query, indexes, returning, generateErr := generateInsertExistOrNotQuery(ctx, spec, false)
	if generateErr != nil {
		err = errors.Warning("sql: new insert when not exist generic failed").WithCause(generateErr).WithMeta("table", spec.Key)
		return
	}

	generic = &InsertWhenNotExistsGeneric{
		spec:      spec,
		method:    method,
		content:   query,
		values:    indexes,
		returning: returning,
	}
	return
}

type InsertWhenNotExistsGeneric struct {
	spec      *specifications.Specification
	method    specifications.Method
	content   []byte
	values    []int
	returning []int
}

func (generic *InsertWhenNotExistsGeneric) Render(ctx specifications.Context, w io.Writer, src specifications.QueryExpr) (method specifications.Method, fields []int, arguments []any, returning []int, err error) {
	method = generic.method
	fields = generic.values
	ctx.SkipNextQueryPlaceholderCursor(len(generic.values))

	srcBuf := bytebufferpool.Get()
	defer bytebufferpool.Put(srcBuf)
	arguments, err = src.Render(ctx, srcBuf)
	if err != nil {
		return
	}
	srcQuery := srcBuf.Bytes()

	query := bytes.Replace(generic.content, srcPlaceHold, srcQuery, 1)
	_, err = w.Write(query)
	if err != nil {
		return
	}

	returning = generic.returning
	return
}
