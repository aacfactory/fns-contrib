package inserts

import (
	"fmt"
	"github.com/aacfactory/errors"
	"github.com/aacfactory/fns-contrib/databases/sql/dac/specifications"
	"github.com/valyala/bytebufferpool"
)

func generateInsertQuery(ctx specifications.Context, spec *specifications.Specification) (method specifications.Method, query []byte, indexes []int, err error) {
	method = specifications.QueryMethod
	buf := bytebufferpool.Get()
	defer bytebufferpool.Put(buf)

	// name
	tableName := ctx.FormatIdent([]byte(spec.Name))
	if spec.Schema != "" {
		schema := ctx.FormatIdent([]byte(spec.Schema))
		schema = append(schema, '.')
		tableName = append(schema, tableName...)
	}
	_, _ = buf.Write(specifications.INSERT)
	_, _ = buf.Write(specifications.SPACE)
	_, _ = buf.Write(specifications.INTO)
	_, _ = buf.Write(specifications.SPACE)
	_, _ = buf.Write(tableName)
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
	var pkName []byte
	if !pk.Incr() {
		pkName = ctx.FormatIdent([]byte(pk.Name))
		_, _ = buf.Write(pkName)
		indexes = append(indexes, pk.FieldIdx)
		n++
	}
	// ver
	ver, hasVer := spec.AuditVersion()
	if hasVer {
		verName := ctx.FormatIdent([]byte(ver.Name))
		if n > 0 {
			_, _ = buf.Write(specifications.COMMA)
		}
		_, _ = buf.Write(verName)
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
		columnName := ctx.FormatIdent([]byte(column.Name))
		if n > 0 {
			_, _ = buf.Write(specifications.COMMA)
		}
		_, _ = buf.Write(columnName)
		indexes = append(indexes, column.FieldIdx)
		n++
	}

	_, _ = buf.Write(specifications.RB)
	_, _ = buf.Write(specifications.SPACE)

	// values
	_, _ = buf.Write(specifications.VALUES)
	_, _ = buf.Write(specifications.SPACE)
	_, _ = buf.Write(specifications.LB)
	n = 0
	if !pk.Incr() {
		_, _ = buf.Write(ctx.NextQueryPlaceholder())
		n++
	}
	if hasVer {
		if n > 0 {
			_, _ = buf.Write(specifications.COMMA)
		}
		_, _ = buf.Write([]byte("1"))
		n++
	}
	for i := 0; i < len(indexes); i++ {
		if n > 0 {
			_, _ = buf.Write(specifications.COMMA)
		}
		_, _ = buf.Write(ctx.NextQueryPlaceholder())
		n++
	}
	_, _ = buf.Write(specifications.RB)

	query = buf.Bytes()

	return
}

var (
	srcPlaceHold = []byte("$$SOURCE_QUERY$$")
)

func generateInsertExistOrNotQuery(ctx specifications.Context, spec *specifications.Specification, exist bool) (method specifications.Method, query []byte, indexes []int, err error) {
	method = specifications.QueryMethod
	buf := bytebufferpool.Get()
	defer bytebufferpool.Put(buf)
	// name
	tableName := ctx.FormatIdent([]byte(spec.Name))
	if spec.Schema != "" {
		schema := ctx.FormatIdent([]byte(spec.Schema))
		schema = append(schema, '.')
		tableName = append(schema, tableName...)
	}
	_, _ = buf.Write(specifications.INSERT)
	_, _ = buf.Write(specifications.SPACE)
	_, _ = buf.Write(specifications.INTO)
	_, _ = buf.Write(specifications.SPACE)
	_, _ = buf.Write(tableName)

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
	var pkName []byte
	if !pk.Incr() {
		pkName = ctx.FormatIdent([]byte(pk.Name))
		_, _ = buf.Write(pkName)
		indexes = append(indexes, pk.FieldIdx)
		n++
	}
	// ver
	ver, hasVer := spec.AuditVersion()
	if hasVer {
		verName := ctx.FormatIdent([]byte(ver.Name))
		if n > 0 {
			_, _ = buf.Write(specifications.COMMA)
		}
		_, _ = buf.Write(verName)
		n++
	}
	// columns
	for _, column := range spec.Columns {
		skip := column.Kind == specifications.Pk || column.Kind == specifications.Aol ||
			column.Kind == specifications.Amb || column.Kind == specifications.Amt ||
			column.Kind == specifications.Adb || column.Kind == specifications.Adt ||
			column.Kind == specifications.Virtual ||
			column.Kind == specifications.Link || column.Kind == specifications.Links
		if skip {
			continue
		}
		columnName := ctx.FormatIdent([]byte(column.Name))
		if n > 0 {
			_, _ = buf.Write(specifications.COMMA)
		}
		_, _ = buf.Write(columnName)
		indexes = append(indexes, column.FieldIdx)
		n++
	}

	_, _ = buf.Write(specifications.RB)
	_, _ = buf.Write(specifications.SPACE)

	// select
	_, _ = buf.Write(specifications.SELECT)
	_, _ = buf.Write(specifications.SPACE)

	n = 0
	if !pk.Incr() {
		_, _ = buf.Write(ctx.NextQueryPlaceholder())
		n++
	}

	if hasVer {
		if n > 0 {
			_, _ = buf.Write(specifications.COMMA)
		}
		_, _ = buf.Write([]byte("1"))
		n++
	}

	for i := 0; i < len(indexes); i++ {
		if n > 0 {
			_, _ = buf.Write(specifications.COMMA)
		}
		_, _ = buf.Write(ctx.NextQueryPlaceholder())
		n++
	}
	_, _ = buf.Write(specifications.SPACE)
	_, _ = buf.Write(specifications.FORM)
	_, _ = buf.Write(specifications.SPACE)
	_, _ = buf.Write(specifications.LB)
	_, _ = buf.Write(specifications.SELECT)
	_, _ = buf.Write(specifications.SPACE)
	_, _ = buf.Write([]byte("1"))
	_, _ = buf.Write(specifications.RB)
	_, _ = buf.Write(specifications.SPACE)
	_, _ = buf.Write(specifications.AS)
	_, _ = buf.Write(specifications.SPACE)
	_, _ = buf.Write(ctx.FormatIdent([]byte("__TMP__")))
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
	_, _ = buf.Write(specifications.FORM)
	_, _ = buf.Write(specifications.SPACE)
	_, _ = buf.Write(specifications.LB)
	_, _ = buf.Write(srcPlaceHold)
	_, _ = buf.Write(specifications.RB)
	_, _ = buf.Write(specifications.SPACE)
	_, _ = buf.Write(specifications.AS)
	_, _ = buf.Write(specifications.SPACE)
	_, _ = buf.Write(ctx.FormatIdent([]byte("__SRC__")))
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

	return
}
