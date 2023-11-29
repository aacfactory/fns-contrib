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
	query, vr, indexes, returning, generateErr := generateInsertQuery(ctx, spec)
	if generateErr != nil {
		err = errors.Warning("sql: new insert generic failed").WithCause(generateErr).WithMeta("table", spec.Key)
		return
	}

	buf := bytebufferpool.Get()
	defer bytebufferpool.Put(buf)
	// conflict
	var conflictFragment []byte
	conflicts := spec.Conflicts
	var conflictFieldIndexes []int
	conflictColumns := make([][]byte, 0, 1)
	if len(conflicts) > 0 {
		_, _ = buf.Write(specifications.SPACE)
		_, _ = buf.Write(specifications.ON)
		_, _ = buf.Write(specifications.SPACE)
		_, _ = buf.Write(specifications.CONFLICT)
		_, _ = buf.Write(specifications.SPACE)
		_, _ = buf.Write(specifications.LB)
		conflictFieldIndexes = make([]int, 0, len(conflicts))
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
			conflictColumn := ctx.FormatIdent([]byte(cc.Name))
			conflictColumns = append(conflictColumns, conflictColumn)
			conflictFieldIndexes = append(conflictFieldIndexes, cc.FieldIdx)
			_, _ = buf.Write(conflictColumn)
			n++
		}
		_, _ = buf.Write(specifications.RB)
		_, _ = buf.Write(specifications.SPACE)
		_, _ = buf.Write(specifications.DO)
		_, _ = buf.Write(specifications.SPACE)
		_, _ = buf.Write(specifications.NOTHING)
		conflictFragment = buf.Bytes()
		buf.Reset()
	}

	// returning
	var returningFragment []byte
	if len(returning) > 0 {
		method = specifications.QueryMethod
		_, _ = buf.Write(specifications.SPACE)
		_, _ = buf.Write(specifications.RETURNING)
		_, _ = buf.Write(specifications.SPACE)
		for i, r := range returning {
			if i > 0 {
				_, _ = buf.Write(specifications.COMMA)
			}
			column, has := spec.ColumnByFieldIdx(r)
			if has {
				_, _ = buf.Write(ctx.FormatIdent([]byte(column.Name)))
			}
		}
		if len(conflicts) > 0 {
			for _, conflictColumn := range conflictColumns {
				_, _ = buf.Write(specifications.COMMA)
				_, _ = buf.Write(conflictColumn)
			}
			returning = append(returning, conflictFieldIndexes...)
		}

		returningFragment = buf.Bytes()
	}

	generic = &InsertGeneric{
		spec:              spec,
		method:            method,
		content:           query,
		vr:                vr,
		conflictFragment:  conflictFragment,
		returningFragment: returningFragment,
		returning:         returning,
		values:            indexes,
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
	returning         []int
	values            []int
}

func (generic *InsertGeneric) Render(ctx specifications.Context, w io.Writer, values int) (method specifications.Method, fields []int, returning []int, err error) {
	method = generic.method
	returning = generic.returning
	fields = generic.values

	buf := bytebufferpool.Get()
	defer bytebufferpool.Put(buf)

	_, _ = buf.Write(generic.content)

	for i := 0; i < values; i++ {
		if i > 0 {
			_, _ = buf.Write(specifications.COMMA)
		}
		_ = generic.vr.Render(ctx, buf)
	}

	_, _ = buf.Write(generic.conflictFragment)
	_, _ = buf.Write(generic.returningFragment)

	query := buf.Bytes()

	_, err = w.Write(query)
	if err != nil {
		return
	}

	return
}
