package inserts

import (
	"fmt"
	"github.com/aacfactory/errors"
	"github.com/aacfactory/fns-contrib/databases/sql/dac/specifications"
	"github.com/valyala/bytebufferpool"
	"io"
)

var (
	IGNORE = []byte("IGNORE")
)

func NewInsertGeneric(ctx specifications.Context, spec *specifications.Specification) (generic *InsertGeneric, err error) {
	if spec.View {
		generic = &InsertGeneric{}
		return
	}
	method := specifications.ExecuteMethod
	query, vr, fields, returning, generateErr := generateInsertQuery(ctx, spec)
	if generateErr != nil {
		err = errors.Warning("sql: new insert generic failed").WithCause(generateErr).WithMeta("table", spec.Key)
		return
	}

	query = query[6:]

	// conflict
	conflictColumns := make([][]byte, 0, 1)
	conflictFields := make([]string, 0, 1)
	if len(spec.Conflicts) > 0 {
		for _, conflict := range spec.Conflicts {
			cc, hasCC := spec.ColumnByField(conflict)
			if !hasCC {
				err = errors.Warning("sql: new insert generic failed").
					WithCause(errors.Warning(fmt.Sprintf("column was not found by %s field", conflict))).WithMeta("table", spec.Key)
				return
			}
			conflictColumn := ctx.FormatIdent(cc.Name)
			conflictColumns = append(conflictColumns, []byte(conflictColumn))
			conflictFields = append(conflictFields, cc.Field)
		}
	}

	// returning
	var returningFragment []byte
	if len(returning) > 0 {
		method = specifications.QueryMethod
		buf := bytebufferpool.Get()
		_, _ = buf.Write(specifications.SPACE)
		_, _ = buf.Write(specifications.RETURNING)
		_, _ = buf.Write(specifications.SPACE)
		for i, r := range returning {
			if i > 0 {
				_, _ = buf.Write(specifications.COMMA)
			}
			column, has := spec.ColumnByField(r)
			if has {
				_, _ = buf.WriteString(ctx.FormatIdent(column.Name))
			}
		}
		if len(conflictColumns) > 0 {
			for _, conflictColumn := range conflictColumns {
				_, _ = buf.Write(specifications.COMMA)
				_, _ = buf.Write(conflictColumn)
			}
			returning = append(returning, conflictFields...)
		}
		returningFragment = []byte(buf.String())
		bytebufferpool.Put(buf)
	}

	generic = &InsertGeneric{
		spec:              spec,
		method:            method,
		content:           query,
		vr:                vr,
		returningFragment: returningFragment,
		returning:         returning,
		fields:            fields,
	}
	return
}

type InsertGeneric struct {
	spec              *specifications.Specification
	method            specifications.Method
	content           []byte
	vr                ValueRender
	returningFragment []byte
	returning         []string
	fields            []string
}

func (generic *InsertGeneric) Render(ctx specifications.Context, w io.Writer, values int) (method specifications.Method, fields []string, returning []string, err error) {
	method = generic.method
	returning = generic.returning
	fields = generic.fields

	_, _ = w.Write(specifications.INSERT)

	if len(generic.spec.Conflicts) > 0 && values == 1 {
		_, _ = w.Write(specifications.SPACE)
		_, _ = w.Write(IGNORE)
		_, _ = w.Write(specifications.SPACE)
	}

	_, _ = w.Write(generic.content)

	for i := 0; i < values; i++ {
		if i > 0 {
			_, _ = w.Write(specifications.COMMA)
		}
		_ = generic.vr.Render(ctx, w)
	}

	_, _ = w.Write(generic.returningFragment)

	return
}
