package inserts

import (
	"fmt"
	"github.com/aacfactory/errors"
	"github.com/aacfactory/fns-contrib/databases/sql/dac/specifications"
	"github.com/valyala/bytebufferpool"
)

func generateInsertQuery(ctx specifications.Context, spec *specifications.Specification) (query string, vr ValueRender, fields []string, returning []string, err error) {
	vr = NewValueRender()
	buf := bytebufferpool.Get()
	defer bytebufferpool.Put(buf)

	// name
	tableName := ctx.FormatIdent(spec.Name)
	if spec.Schema != "" {
		schema := ctx.FormatIdent(spec.Schema)
		tableName = fmt.Sprintf("%s.%s", schema, tableName)
	}
	_, _ = buf.Write(specifications.INSERT)
	_, _ = buf.Write(specifications.SPACE)
	_, _ = buf.Write(specifications.INTO)
	_, _ = buf.Write(specifications.SPACE)
	_, _ = buf.WriteString(tableName)
	_, _ = buf.Write(specifications.SPACE)

	// column
	_, _ = buf.Write(specifications.SPACE)
	_, _ = buf.Write(specifications.LB)
	n := 0
	// pk
	pk, hasPk := spec.Pk()
	if !hasPk {
		err = errors.Warning("pk is required")
		return
	}
	pkName := ""
	if pk.Incr() {
		returning = append(returning, pk.Field)
	} else {
		pkName = ctx.FormatIdent(pk.Name)
		_, _ = buf.WriteString(pkName)
		vr.Add()
		fields = append(fields, pk.Field)
		n++
	}
	// ver
	ver, hasVer := spec.AuditVersion()
	if hasVer {
		verName := ctx.FormatIdent(ver.Name)
		if n > 0 {
			_, _ = buf.Write(specifications.COMMA)
		}
		_, _ = buf.WriteString(verName)
		vr.Add()
		vr.MarkAsVersion()
		n++
	}
	for _, column := range spec.Columns {
		skip := column.Kind == specifications.Pk || column.Kind == specifications.Aol ||
			column.Kind == specifications.Amb || column.Kind == specifications.Amt ||
			column.Kind == specifications.Adb || column.Kind == specifications.Adt ||
			column.Kind == specifications.Virtual ||
			column.Kind == specifications.Link || column.Kind == specifications.Links
		if skip {
			continue
		}
		if column.Incr() {
			returning = append(returning, column.Field)
			continue
		}

		columnName := ctx.FormatIdent(column.Name)
		if n > 0 {
			_, _ = buf.Write(specifications.COMMA)
		}
		_, _ = buf.WriteString(columnName)
		vr.Add()
		fields = append(fields, column.Field)
		n++
	}

	_, _ = buf.Write(specifications.RB)
	_, _ = buf.Write(specifications.SPACE)

	// values
	_, _ = buf.Write(specifications.VALUES)
	_, _ = buf.Write(specifications.SPACE)

	query = buf.String()
	return
}

var (
	srcPlaceHold = []byte("$$SOURCE_QUERY$$")
)

func generateInsertExistOrNotQuery(ctx specifications.Context, spec *specifications.Specification, exist bool) (method specifications.Method, query string, fields []string, returning []string, err error) {
	method = specifications.ExecuteMethod
	buf := bytebufferpool.Get()
	defer bytebufferpool.Put(buf)
	// name
	tableName := ctx.FormatIdent(spec.Name)
	if spec.Schema != "" {
		schema := ctx.FormatIdent(spec.Schema)
		tableName = fmt.Sprintf("%s.%s", schema, tableName)
	}
	_, _ = buf.Write(specifications.INSERT)
	_, _ = buf.Write(specifications.SPACE)
	_, _ = buf.Write(specifications.INTO)
	_, _ = buf.Write(specifications.SPACE)
	_, _ = buf.WriteString(tableName)

	// column
	_, _ = buf.Write(specifications.SPACE)
	_, _ = buf.Write(specifications.LB)

	n := 0
	// pk
	pk, hasPk := spec.Pk()
	if !hasPk {
		err = errors.Warning("pk is required")
		return
	}
	pkName := ""
	if pk.Incr() {
		returning = append(returning, pk.Field)
	} else {
		pkName = ctx.FormatIdent(pk.Name)
		_, _ = buf.WriteString(pkName)
		fields = append(fields, pk.Field)
		n++
	}

	// ver
	ver, hasVer := spec.AuditVersion()
	if hasVer {
		verName := ctx.FormatIdent(ver.Name)
		if n > 0 {
			_, _ = buf.Write(specifications.COMMA)
		}
		_, _ = buf.WriteString(verName)
		n++
	}
	// columns
	columnsLen := 0
	for _, column := range spec.Columns {
		skip := column.Kind == specifications.Pk || column.Kind == specifications.Aol ||
			column.Kind == specifications.Amb || column.Kind == specifications.Amt ||
			column.Kind == specifications.Adb || column.Kind == specifications.Adt ||
			column.Kind == specifications.Virtual ||
			column.Kind == specifications.Link || column.Kind == specifications.Links
		if skip {
			continue
		}
		if column.Incr() {
			returning = append(returning, column.Field)
			continue
		}
		columnName := ctx.FormatIdent(column.Name)
		if n > 0 {
			_, _ = buf.Write(specifications.COMMA)
		}
		_, _ = buf.WriteString(columnName)
		fields = append(fields, column.Field)
		columnsLen++
		n++
	}

	_, _ = buf.Write(specifications.RB)
	_, _ = buf.Write(specifications.SPACE)

	// select
	_, _ = buf.Write(specifications.SELECT)
	_, _ = buf.Write(specifications.SPACE)

	n = 0
	if !pk.Incr() {
		_, _ = buf.WriteString(ctx.NextQueryPlaceholder())
		n++
	}

	if hasVer {
		if n > 0 {
			_, _ = buf.Write(specifications.COMMA)
		}
		_, _ = buf.Write([]byte("1"))
		n++
	}

	for i := 0; i < columnsLen; i++ {
		if n > 0 {
			_, _ = buf.Write(specifications.COMMA)
		}
		_, _ = buf.WriteString(ctx.NextQueryPlaceholder())
		n++
	}
	_, _ = buf.Write(specifications.SPACE)
	_, _ = buf.Write(specifications.FROM)
	_, _ = buf.Write(specifications.SPACE)
	_, _ = buf.Write(specifications.LB)
	_, _ = buf.Write(specifications.SELECT)
	_, _ = buf.Write(specifications.SPACE)
	_, _ = buf.Write([]byte("1"))
	_, _ = buf.Write(specifications.RB)
	_, _ = buf.Write(specifications.SPACE)
	_, _ = buf.Write(specifications.AS)
	_, _ = buf.Write(specifications.SPACE)
	_, _ = buf.WriteString(ctx.FormatIdent("__TMP__"))
	_, _ = buf.Write(specifications.SPACE)
	_, _ = buf.Write(specifications.WHERE)
	_, _ = buf.Write(specifications.SPACE)
	if !exist {
		_, _ = buf.Write(specifications.NOT)
		_, _ = buf.Write(specifications.SPACE)
	}
	_, _ = buf.Write(specifications.EXISTS)
	_, _ = buf.Write(specifications.SPACE)

	// source
	_, _ = buf.Write(specifications.LB)
	_, _ = buf.Write(specifications.SELECT)
	_, _ = buf.Write(specifications.SPACE)
	_, _ = buf.Write([]byte("1"))
	_, _ = buf.Write(specifications.SPACE)
	_, _ = buf.Write(specifications.FROM)
	_, _ = buf.Write(specifications.SPACE)
	//_, _ = buf.Write(specifications.LB)
	_, _ = buf.Write(srcPlaceHold)
	//_, _ = buf.Write(specifications.RB)
	_, _ = buf.Write(specifications.SPACE)
	_, _ = buf.Write(specifications.AS)
	_, _ = buf.Write(specifications.SPACE)
	_, _ = buf.WriteString(ctx.FormatIdent("__SRC__"))
	_, _ = buf.Write(specifications.SPACE)
	_, _ = buf.Write(specifications.RB)

	conflicts := spec.Conflicts
	if len(conflicts) > 0 {
		_, _ = buf.Write(specifications.SPACE)
		_, _ = buf.Write(specifications.ON)
		_, _ = buf.Write(specifications.SPACE)
		_, _ = buf.Write(specifications.CONFLICT)
		_, _ = buf.Write(specifications.SPACE)
		_, _ = buf.Write(specifications.LB)
		n = 0
		for _, conflict := range conflicts {
			cc, hasCC := spec.ColumnByField(conflict)
			if !hasCC {
				err = errors.Warning(fmt.Sprintf("column was not found by %s field", conflict))
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
		_, _ = buf.Write(specifications.NOTHING)
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

	return
}
