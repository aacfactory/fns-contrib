package inserts

import (
	"bytes"
	"github.com/aacfactory/errors"
	"github.com/aacfactory/fns-contrib/databases/sql/dac/specifications"
	"github.com/valyala/bytebufferpool"
	"io"
)

func NewInsertWhenExistsGeneric(ctx specifications.Context, spec *specifications.Specification) (generic *InsertWhenExistsGeneric, err error) {
	if spec.View {
		generic = &InsertWhenExistsGeneric{}
		return
	}
	method, query, indexes, generateErr := generateInsertExistOrNotQuery(ctx, spec, true)
	if generateErr != nil {
		err = errors.Warning("sql: new insert when exist generic failed").WithCause(generateErr).WithMeta("table", spec.Key)
		return
	}

	generic = &InsertWhenExistsGeneric{
		spec:    spec,
		method:  method,
		content: query,
		values:  indexes,
	}

	return
}

type InsertWhenExistsGeneric struct {
	spec    *specifications.Specification
	method  specifications.Method
	content []byte
	values  []int
}

func (generic *InsertWhenExistsGeneric) Render(ctx specifications.Context, w io.Writer, instance specifications.Table, src specifications.QueryExpr) (method specifications.Method, arguments []any, err error) {
	method = generic.method

	ctx.SkipNextQueryPlaceholderCursor(len(generic.values))

	srcBuf := bytebufferpool.Get()
	defer bytebufferpool.Put(srcBuf)
	srcArgs, srcErr := src.Render(ctx, srcBuf)
	if srcErr != nil {
		err = srcErr
		return
	}
	srcQuery := srcBuf.Bytes()

	query := bytes.Replace(generic.content, srcPlaceHold, srcQuery, 1)
	_, err = w.Write(query)
	if err != nil {
		return
	}

	arguments, err = generic.spec.Arguments(instance, generic.values)
	if err != nil {
		return
	}

	arguments = append(arguments, arguments...)
	arguments = append(arguments, srcArgs...)

	return
}
