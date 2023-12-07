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
	method := specifications.ExecuteMethod
	query, vr, fields, returning, generateErr := generateInsertQuery(ctx, spec)
	if generateErr != nil {
		err = errors.Warning("sql: new insert generic failed").WithCause(generateErr).WithMeta("table", spec.Key)
		return
	}

	// conflict
	var conflictFragment string
	conflicts := spec.Conflicts
	var conflictFields []string
	conflictColumns := make([]string, 0, 1)
	if len(conflicts) > 0 {
		buf := bytebufferpool.Get()
		_, _ = buf.Write(specifications.SPACE)
		_, _ = buf.Write(specifications.ON)
		_, _ = buf.Write(specifications.SPACE)
		_, _ = buf.Write(specifications.CONFLICT)
		_, _ = buf.Write(specifications.SPACE)
		_, _ = buf.Write(specifications.LB)
		conflictFields = make([]string, 0, len(conflicts))
		n := 0
		for _, conflict := range conflicts {
			cc, hasCC := spec.ColumnByField(conflict)
			if !hasCC {
				bytebufferpool.Put(buf)
				err = errors.Warning("sql: new insert generic failed").
					WithCause(errors.Warning(fmt.Sprintf("column was not found by %s field", conflict))).WithMeta("table", spec.Key)
				return
			}
			if n > 0 {
				_, _ = buf.Write(specifications.COMMA)
			}
			conflictColumn := ctx.FormatIdent(cc.Name)
			conflictColumns = append(conflictColumns, conflictColumn)
			conflictFields = append(conflictFields, cc.Field)
			_, _ = buf.WriteString(conflictColumn)
			n++
		}
		_, _ = buf.Write(specifications.RB)
		_, _ = buf.Write(specifications.SPACE)
		_, _ = buf.Write(specifications.DO)
		_, _ = buf.Write(specifications.SPACE)
		_, _ = buf.Write(specifications.NOTHING)
		conflictFragment = buf.String()
		bytebufferpool.Put(buf)
	}

	// returning
	var returningFragment string
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
		if len(conflicts) > 0 {
			for _, conflictColumn := range conflictColumns {
				_, _ = buf.Write(specifications.COMMA)
				_, _ = buf.WriteString(conflictColumn)
			}
			returning = append(returning, conflictFields...)
		}
		returningFragment = buf.String()
		bytebufferpool.Put(buf)
	}

	generic = &InsertGeneric{
		spec:              spec,
		method:            method,
		content:           []byte(query),
		vr:                vr,
		conflictFragment:  []byte(conflictFragment),
		returningFragment: []byte(returningFragment),
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
	conflictFragment  []byte
	returningFragment []byte
	returning         []string
	fields            []string
}

func (generic *InsertGeneric) Render(ctx specifications.Context, w io.Writer, values int) (method specifications.Method, fields []string, returning []string, err error) {
	method = generic.method
	returning = generic.returning
	fields = generic.fields

	_, _ = w.Write(generic.content)

	for i := 0; i < values; i++ {
		if i > 0 {
			_, _ = w.Write(specifications.COMMA)
		}
		_ = generic.vr.Render(ctx, w)
	}

	_, _ = w.Write(generic.conflictFragment)
	_, _ = w.Write(generic.returningFragment)

	return
}
