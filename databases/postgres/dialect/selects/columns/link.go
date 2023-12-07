package columns

import (
	"fmt"
	"github.com/aacfactory/errors"
	"github.com/aacfactory/fns-contrib/databases/sql/dac/specifications"
	"github.com/valyala/bytebufferpool"
)

func Link(ctx specifications.Context, spec *specifications.Specification, column *specifications.Column) (fragment string, err error) {
	buf := bytebufferpool.Get()
	defer bytebufferpool.Put(buf)

	hostTableName := ctx.FormatIdent(spec.Name)
	if spec.Schema != "" {
		hostSchemaName := ctx.FormatIdent(spec.Schema)
		hostTableName = fmt.Sprintf("%s.%s", hostSchemaName, hostTableName)
	}

	hostField, awayField, mapping, ok := column.Link()
	if !ok {
		err = errors.Warning("sql: render link field failed").
			WithCause(fmt.Errorf("%s is not link", column.Field)).
			WithMeta("table", spec.Key).
			WithMeta("field", column.Field)
		return
	}
	hostColumn, hasHostColumn := spec.ColumnByField(hostField)
	if !hasHostColumn {
		err = errors.Warning("sql: render link field failed").
			WithCause(fmt.Errorf("%s is not found in %s", hostField, spec.Key)).
			WithMeta("table", spec.Key).
			WithMeta("field", column.Field)
		return
	}

	awayColumn, hasAwayColumn := mapping.ColumnByField(awayField)
	if !hasAwayColumn {
		err = errors.Warning("sql: render link field failed").
			WithCause(fmt.Errorf("%s is not found in %s", awayField, mapping.Key)).
			WithMeta("table", spec.Key).
			WithMeta("field", column.Field)
		return
	}
	awayColumnName := ctx.FormatIdent(awayColumn.Name)

	awayTableName := ctx.FormatIdent(mapping.Name)
	if mapping.Schema != "" {
		awaySchemaName := ctx.FormatIdent(mapping.Schema)
		awayTableName = fmt.Sprintf("%s.%s", awaySchemaName, awayTableName)
	}

	srcName := ctx.FormatIdent(fmt.Sprintf("%s_%s", spec.Name, mapping.Name))

	_, _ = buf.Write(specifications.LB) // (
	// json >>>
	_, _ = buf.Write(specifications.SELECT)
	_, _ = buf.Write(specifications.SPACE)
	_, _ = buf.WriteString("row_to_json")
	_, _ = buf.Write(specifications.LB)
	_, _ = buf.WriteString(srcName)
	_, _ = buf.Write(specifications.DOT)
	_, _ = buf.Write(specifications.STAR)
	_, _ = buf.Write(specifications.RB)
	_, _ = buf.Write(specifications.SPACE)
	_, _ = buf.Write(specifications.FROM)
	_, _ = buf.Write(specifications.SPACE)

	// src >>>
	_, _ = buf.Write(specifications.LB)
	_, _ = buf.Write(specifications.SELECT)
	_, _ = buf.Write(specifications.SPACE)
	for i, mappingColumn := range mapping.Columns {
		if i > 0 {
			_, _ = buf.Write(specifications.COMMA)
		}
		switch mappingColumn.Kind {
		case specifications.Reference, specifications.Link, specifications.Links, specifications.Virtual:
			mappingColumnFragment, fragmentErr := Fragment(ctx, mapping, mappingColumn)
			if fragmentErr != nil {
				err = fragmentErr
				return
			}
			_, _ = buf.WriteString(mappingColumnFragment)
			break
		default:
			_, _ = buf.WriteString(ctx.FormatIdent(mappingColumn.Name))
			_, _ = buf.Write(specifications.SPACE)
			_, _ = buf.Write(specifications.AS)
			_, _ = buf.Write(specifications.SPACE)
			_, _ = buf.WriteString(ctx.FormatIdent(mappingColumn.JsonIdent))
		}
	}
	_, _ = buf.Write(specifications.SPACE)
	_, _ = buf.Write(specifications.FROM)
	_, _ = buf.Write(specifications.SPACE)
	_, _ = buf.WriteString(awayTableName)
	_, _ = buf.Write(specifications.SPACE)
	_, _ = buf.Write(specifications.WHERE)
	_, _ = buf.Write(specifications.SPACE)
	_, _ = buf.WriteString(awayColumnName)
	_, _ = buf.Write(specifications.SPACE)
	_, _ = buf.Write(specifications.EQ)
	_, _ = buf.Write(specifications.SPACE)
	_, _ = buf.WriteString(hostTableName)
	_, _ = buf.Write(specifications.DOT)
	_, _ = buf.WriteString(ctx.FormatIdent(hostColumn.Name))
	_, _ = buf.Write(specifications.SPACE)
	_, _ = buf.Write(specifications.OFFSET)
	_, _ = buf.Write(specifications.SPACE)
	_, _ = buf.WriteString("0")
	_, _ = buf.Write(specifications.SPACE)
	_, _ = buf.Write(specifications.LIMIT)
	_, _ = buf.Write(specifications.SPACE)
	_, _ = buf.WriteString("1")
	_, _ = buf.Write(specifications.RB)
	_, _ = buf.Write(specifications.SPACE)
	_, _ = buf.Write(specifications.AS)
	_, _ = buf.Write(specifications.SPACE)
	_, _ = buf.WriteString(srcName)
	// json <<<
	_, _ = buf.Write(specifications.RB)
	_, _ = buf.Write(specifications.SPACE)
	_, _ = buf.Write(specifications.AS)
	_, _ = buf.Write(specifications.SPACE)
	_, _ = buf.WriteString(ctx.FormatIdent(column.Name))

	fragment = buf.String()
	return
}
