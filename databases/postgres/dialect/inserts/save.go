package inserts

import (
	"fmt"
	"github.com/aacfactory/errors"
	"github.com/aacfactory/fns-contrib/databases/sql/dac/specifications"
	"github.com/valyala/bytebufferpool"
	"io"
)

func NewInsertOrUpdateGeneric(ctx specifications.Context, spec *specifications.Specification) (generic *InsertOrUpdateGeneric, err error) {
	if spec.View {
		generic = &InsertOrUpdateGeneric{}
		return
	}
	method := specifications.ExecuteMethod

	query, vr, fields, returning, generateErr := generateInsertQuery(ctx, spec)
	if generateErr != nil {
		err = errors.Warning("sql: new insert or update generic failed").WithCause(generateErr).WithMeta("table", spec.Key)
		return
	}

	buf := bytebufferpool.Get()
	defer bytebufferpool.Put(buf)
	_, _ = buf.WriteString(query)
	_ = vr.Render(ctx, buf)

	// conflict
	conflicts := spec.Conflicts
	if len(conflicts) > 0 {
		// name
		tableName := ctx.FormatIdent(spec.Name)
		if spec.Schema != "" {
			schema := ctx.FormatIdent(spec.Schema)
			tableName = fmt.Sprintf("%s.%s", schema, tableName)
		}

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
				err = errors.Warning("sql: new insert or update generic failed").
					WithCause(errors.Warning(fmt.Sprintf("column was not found by %s field", conflict))).WithMeta("table", spec.Key)
				return
			}
			if n > 0 {
				_, _ = buf.Write(specifications.COMMA)
			}
			_, _ = buf.WriteString(ctx.FormatIdent(cc.Name))
			n++
		}

		_, _ = buf.Write(specifications.RB)
		_, _ = buf.Write(specifications.SPACE)
		_, _ = buf.Write(specifications.DO)
		_, _ = buf.Write(specifications.SPACE)
		_, _ = buf.Write(specifications.UPDATE)
		_, _ = buf.Write(specifications.SPACE)
		_, _ = buf.Write(specifications.SET)
		_, _ = buf.Write(specifications.SPACE)

		n = 0
		for _, column := range spec.Columns {
			skip := column.Kind == specifications.Pk ||
				column.Kind == specifications.Acb || column.Kind == specifications.Act ||
				column.Kind == specifications.Adb || column.Kind == specifications.Adt ||
				column.Kind == specifications.Virtual ||
				column.Kind == specifications.Link || column.Kind == specifications.Links
			if skip {
				continue
			}
			if column.Kind == specifications.Aol {
				if n > 0 {
					_, _ = buf.Write(specifications.COMMA)
				}
				verName := ctx.FormatIdent(column.Name)
				_, _ = buf.WriteString(verName)
				_, _ = buf.Write(specifications.SPACE)
				_, _ = buf.Write(specifications.EQ)
				_, _ = buf.Write(specifications.SPACE)
				_, _ = buf.WriteString(tableName)
				_, _ = buf.Write(specifications.DOT)
				_, _ = buf.WriteString(verName)
				_, _ = buf.Write(specifications.PLUS)
				_, _ = buf.Write([]byte("1"))
				n++
				continue
			}
			if n > 0 {
				_, _ = buf.Write(specifications.COMMA)
			}
			columnName := ctx.FormatIdent(column.Name)
			_, _ = buf.WriteString(columnName)
			_, _ = buf.Write(specifications.SPACE)
			_, _ = buf.Write(specifications.EQ)
			_, _ = buf.Write(specifications.SPACE)
			_, _ = buf.WriteString(ctx.NextQueryPlaceholder())
			fields = append(fields, column.Field)
			n++
		}

	}

	// returning
	if len(returning) > 0 {
		method = specifications.QueryMethod
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
	}

	query = buf.String()

	generic = &InsertOrUpdateGeneric{
		spec:      spec,
		method:    method,
		content:   query,
		returning: returning,
		fields:    fields,
	}
	return
}

type InsertOrUpdateGeneric struct {
	spec      *specifications.Specification
	method    specifications.Method
	content   string
	returning []string
	fields    []string
}

func (generic *InsertOrUpdateGeneric) Render(_ specifications.Context, w io.Writer) (method specifications.Method, fields []string, returning []string, err error) {
	method = generic.method
	returning = generic.returning
	fields = generic.fields

	_, err = w.Write([]byte(generic.content))
	if err != nil {
		return
	}

	return
}
