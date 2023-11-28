package inserts

import (
	"fmt"
	"github.com/aacfactory/errors"
	"github.com/aacfactory/fns-contrib/databases/sql/dac/specifications"
	"github.com/valyala/bytebufferpool"
	"io"
)

func NewInsertGeneric(ctx specifications.Context, spec *specifications.Specification) (generic *InsertGeneric, err error) {
	if spec.View {
		generic = &InsertGeneric{}
		return
	}
	method, query, indexes, generateErr := generateInsertExistOrNotQuery(ctx, spec, true)
	if generateErr != nil {
		err = errors.Warning("sql: new insert generic failed").WithCause(generateErr).WithMeta("table", spec.Key)
		return
	}

	buf := bytebufferpool.Get()
	defer bytebufferpool.Put(buf)
	_, _ = buf.Write(query)
	// conflict
	conflicts := spec.Conflicts
	if len(conflicts) > 0 {
		_, _ = buf.Write(specifications.SPACE)
		_, _ = buf.Write(specifications.ON)
		_, _ = buf.Write(specifications.SPACE)
		_, _ = buf.Write(specifications.CONFLICT)
		_, _ = buf.Write(specifications.SPACE)
		_, _ = buf.Write(specifications.LB)
		n := 0
		for _, conflict := range conflicts {
			cc, hasCC := spec.ColumnByField(conflict)
			if !hasCC {
				err = errors.Warning("sql: new insert generic failed").
					WithCause(errors.Warning(fmt.Sprintf("column was not found by %s field", conflict))).WithMeta("table", spec.Key)
				return
			}
			if n > 0 {
				_, _ = buf.Write(specifications.COMMA)
			}
			_, _ = buf.Write(ctx.FormatIdent([]byte(cc.Name)))
			n++
		}
		_, _ = buf.Write(specifications.RB)
		_, _ = buf.Write(specifications.SPACE)
		_, _ = buf.Write(specifications.DO)
		_, _ = buf.Write(specifications.SPACE)
		_, _ = buf.Write(specifications.NOTHING)
	}

	// incr
	pk, hasPk := spec.Pk()
	if !hasPk {
		err = errors.Warning("sql: new insert generic failed").WithCause(errors.Warning("pk is required")).WithMeta("table", spec.Key)
		return
	}
	pkName := ctx.FormatIdent([]byte(pk.Name))
	if pk.Incr() {
		method = specifications.ExecuteMethod
		_, _ = buf.Write(specifications.SPACE)
		_, _ = buf.Write(specifications.RETURNING)
		_, _ = buf.Write(specifications.SPACE)
		_, _ = buf.Write(pkName)
		_, _ = buf.Write(specifications.SPACE)
		_, _ = buf.Write(specifications.AS)
		_, _ = buf.Write(ctx.FormatIdent([]byte("LAST_INSERT_ID")))
	}

	query = buf.Bytes()

	generic = &InsertGeneric{
		spec:    spec,
		method:  method,
		content: query,
		values:  indexes,
	}
	return
}

type InsertGeneric struct {
	spec    *specifications.Specification
	method  specifications.Method
	content []byte
	values  []int
}

func (generic *InsertGeneric) Render(_ specifications.Context, w io.Writer, instance specifications.Table) (method specifications.Method, arguments []any, err error) {
	method = generic.method

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
