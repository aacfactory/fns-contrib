package columns

import (
	"fmt"
	"github.com/aacfactory/errors"
	"github.com/aacfactory/fns-contrib/databases/sql/dac/specifications"
	"github.com/valyala/bytebufferpool"
	"strconv"
)

// Links
/*
	(
		SELECT JSON_ARRAYAGG(
			JSON_OBJECT('id', id, 'name', name, 'age', age, 'create_at', create_at)
		)
	FROM `fns-test`.`user` ) AS foo;
*/
func Links(ctx specifications.Context, spec *specifications.Specification, column *specifications.Column) (fragment []byte, err error) {
	buf := bytebufferpool.Get()
	defer bytebufferpool.Put(buf)

	hostTableName := ctx.FormatIdent([]byte(spec.Name))
	if spec.Schema != "" {
		hostSchemaName := ctx.FormatIdent([]byte(spec.Schema))
		hostSchemaName = append(hostSchemaName, '.')
		hostTableName = append(hostSchemaName, hostTableName...)
	}
	hostColumnIdent := ctx.FormatIdent([]byte(column.Name))

	hostField, awayField, mapping, orders, length, ok := column.Links()
	if !ok {
		err = errors.Warning("sql: render links field failed").
			WithCause(fmt.Errorf("%s is not links", column.Field)).
			WithMeta("table", spec.Key).
			WithMeta("field", column.Field)
		return
	}
	hostColumn, hasHostColumn := mapping.ColumnByField(hostField)
	if !hasHostColumn {
		err = errors.Warning("sql: render reference field failed").
			WithCause(fmt.Errorf("%s is not found in %s", hostField, mapping.Key)).
			WithMeta("table", spec.Key).
			WithMeta("field", column.Field)
		return
	}
	hostColumnName := ctx.FormatIdent([]byte(hostColumn.Name))

	awayColumn, hasAwayColumn := mapping.ColumnByField(awayField)
	if !hasAwayColumn {
		err = errors.Warning("sql: render reference field failed").
			WithCause(fmt.Errorf("%s is not found in %s", awayField, mapping.Key)).
			WithMeta("table", spec.Key).
			WithMeta("field", column.Field)
		return
	}
	awayColumnName := ctx.FormatIdent([]byte(awayColumn.Name))

	awayTableName := ctx.FormatIdent([]byte(mapping.Name))
	if mapping.Schema != "" {
		awaySchemaName := ctx.FormatIdent([]byte(mapping.Schema))
		awaySchemaName = append(awaySchemaName, '.')
		awayTableName = append(awaySchemaName, awayTableName...)
	}

	_, _ = buf.Write(specifications.LB)
	// json >>>
	_, _ = buf.Write(specifications.SELECT)
	_, _ = buf.Write(specifications.SPACE)
	_, _ = buf.Write([]byte("JSON_ARRAYAGG"))
	_, _ = buf.Write(specifications.LB)
	_, _ = buf.Write([]byte("JSON_OBJECT"))
	_, _ = buf.Write(specifications.LB)
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
			_, _ = buf.Write(mappingColumnFragment)
			break
		default:
			_, _ = buf.Write(ctx.FormatIdent([]byte(mappingColumn.Name)))
			_, _ = buf.Write(specifications.SPACE)
			_, _ = buf.Write(specifications.AS)
			_, _ = buf.Write(specifications.SPACE)
			_, _ = buf.Write(ctx.FormatIdent([]byte(mappingColumn.JsonIdent)))
		}
	}
	_, _ = buf.Write(specifications.RB)
	_, _ = buf.Write(specifications.RB)
	_, _ = buf.Write(specifications.SPACE)
	_, _ = buf.Write(specifications.FROM)
	_, _ = buf.Write(specifications.SPACE)
	_, _ = buf.Write(awayTableName)
	_, _ = buf.Write(specifications.SPACE)
	_, _ = buf.Write(specifications.WHERE)
	_, _ = buf.Write(specifications.SPACE)
	_, _ = buf.Write(awayColumnName)
	_, _ = buf.Write(specifications.SPACE)
	_, _ = buf.Write(specifications.EQ)
	_, _ = buf.Write(specifications.SPACE)
	_, _ = buf.Write(hostTableName)
	_, _ = buf.Write(specifications.DOT)
	_, _ = buf.Write(hostColumnName)

	if len(orders) > 0 {
		_, _ = buf.Write(specifications.SPACE)
		_, _ = buf.Write(specifications.ORDER)
		_, _ = buf.Write(specifications.SPACE)
		_, _ = buf.Write(specifications.BY)
		_, _ = buf.Write(specifications.SPACE)
		for i, order := range orders {
			if i > 0 {
				_, _ = buf.Write(specifications.COMMA)
			}
			mc, hasMC := mapping.ColumnByField(order.Name)
			if !hasMC {
				err = errors.Warning("sql: render reference field failed").
					WithCause(fmt.Errorf("%s is not found in %s", order.Name, mapping.Key)).
					WithMeta("table", spec.Key).
					WithMeta("field", column.Field)
				return
			}
			_, _ = buf.Write(ctx.FormatIdent([]byte(mc.Name)))
			if order.Desc {
				_, _ = buf.Write(specifications.SPACE)
				_, _ = buf.Write(specifications.DESC)
			}
		}
	}
	if length > 0 {
		_, _ = buf.Write(specifications.SPACE)
		_, _ = buf.Write(specifications.OFFSET)
		_, _ = buf.Write(specifications.SPACE)
		_, _ = buf.Write([]byte("0"))
		_, _ = buf.Write(specifications.SPACE)
		_, _ = buf.Write(specifications.LIMIT)
		_, _ = buf.Write(specifications.SPACE)
		_, _ = buf.Write([]byte(strconv.Itoa(length)))
	}

	// json <<<
	_, _ = buf.Write(specifications.RB)
	_, _ = buf.Write(specifications.SPACE)
	_, _ = buf.Write(specifications.AS)
	_, _ = buf.Write(specifications.SPACE)
	_, _ = buf.Write(hostColumnIdent)

	fragment = []byte(buf.String())
	return
}
