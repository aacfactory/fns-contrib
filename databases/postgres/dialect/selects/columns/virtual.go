package columns

import (
	"fmt"
	"github.com/aacfactory/errors"
	"github.com/aacfactory/fns-contrib/databases/sql/dac/specifications"
	"github.com/valyala/bytebufferpool"
)

// Virtual
// (
//
//	 {SELECT to_json(ARRAY(}
//		{SELECT row_to_json("{name}_virtual".*) FROM (}
//		{query}
//		{) AS "{name}_virtual"}
//	 {))}
//
//	) AS {name}
func Virtual(ctx specifications.Context, spec *specifications.Specification, column *specifications.Column) (fragment string, err error) {
	buf := bytebufferpool.Get()
	defer bytebufferpool.Put(buf)

	kind, query, ok := column.Virtual()
	if !ok {
		err = errors.Warning("sql: render virtual field failed").
			WithCause(fmt.Errorf("%s is not virtual", column.Field)).
			WithMeta("table", spec.Key).
			WithMeta("field", column.Field)
		return
	}
	name := ctx.FormatIdent(column.Name)
	switch kind {
	case specifications.BasicVirtualQuery:
		_, _ = buf.Write(specifications.LB)
		_, _ = buf.WriteString(query)
		_, _ = buf.Write(specifications.RB)
		_, _ = buf.Write(specifications.SPACE)
		_, _ = buf.Write(specifications.AS)
		_, _ = buf.Write(specifications.SPACE)
		_, _ = buf.WriteString(name)
		break
	case specifications.ObjectVirtualQuery:
		src := ctx.FormatIdent(fmt.Sprintf("%s_virtual", column.Field))
		_, _ = buf.Write(specifications.LB)
		_, _ = buf.Write(specifications.SELECT)
		_, _ = buf.Write(specifications.SPACE)
		_, _ = buf.WriteString("row_to_json")
		_, _ = buf.Write(specifications.LB)
		_, _ = buf.WriteString(src)
		_, _ = buf.Write(specifications.DOT)
		_, _ = buf.Write(specifications.STAR)
		_, _ = buf.Write(specifications.RB)
		_, _ = buf.Write(specifications.SPACE)
		_, _ = buf.Write(specifications.FROM)
		_, _ = buf.Write(specifications.SPACE)
		_, _ = buf.Write(specifications.LB)
		_, _ = buf.WriteString(query)
		_, _ = buf.Write(specifications.RB)
		_, _ = buf.Write(specifications.SPACE)
		_, _ = buf.Write(specifications.AS)
		_, _ = buf.Write(specifications.SPACE)
		_, _ = buf.WriteString(src)
		_, _ = buf.Write(specifications.SPACE)
		_, _ = buf.Write(specifications.LIMIT)
		_, _ = buf.Write(specifications.SPACE)
		_, _ = buf.WriteString("1")
		_, _ = buf.Write(specifications.RB)
		_, _ = buf.Write(specifications.SPACE)
		_, _ = buf.Write(specifications.AS)
		_, _ = buf.Write(specifications.SPACE)
		_, _ = buf.WriteString(name)
		break
	case specifications.ArrayVirtualQuery:
		src := ctx.FormatIdent(fmt.Sprintf("%s_virtual", column.Field))
		_, _ = buf.Write(specifications.LB)
		_, _ = buf.Write(specifications.SELECT)
		_, _ = buf.Write(specifications.SPACE)
		_, _ = buf.WriteString("to_json")
		_, _ = buf.Write(specifications.LB)
		_, _ = buf.WriteString("ARRAY")
		_, _ = buf.Write(specifications.LB)
		_, _ = buf.Write(specifications.SELECT)
		_, _ = buf.Write(specifications.SPACE)
		_, _ = buf.WriteString("row_to_json")
		_, _ = buf.Write(specifications.LB)
		_, _ = buf.WriteString(src)
		_, _ = buf.Write(specifications.DOT)
		_, _ = buf.Write(specifications.STAR)
		_, _ = buf.Write(specifications.RB)
		_, _ = buf.Write(specifications.SPACE)
		_, _ = buf.Write(specifications.FROM)
		_, _ = buf.Write(specifications.SPACE)
		_, _ = buf.Write(specifications.LB)
		_, _ = buf.WriteString(query)
		_, _ = buf.Write(specifications.RB)
		_, _ = buf.Write(specifications.SPACE)
		_, _ = buf.Write(specifications.AS)
		_, _ = buf.Write(specifications.SPACE)
		_, _ = buf.WriteString(src)
		_, _ = buf.Write(specifications.RB)
		_, _ = buf.Write(specifications.RB)
		_, _ = buf.Write(specifications.RB)
		_, _ = buf.Write(specifications.SPACE)
		_, _ = buf.Write(specifications.AS)
		_, _ = buf.Write(specifications.SPACE)
		_, _ = buf.WriteString(name)
		break
	case specifications.AggregateVirtualQuery:
		_, _ = buf.Write([]byte(query))
		_, _ = buf.Write(specifications.LB)
		_, _ = buf.WriteString(name)
		_, _ = buf.Write(specifications.RB)
		_, _ = buf.Write(specifications.SPACE)
		_, _ = buf.Write(specifications.AS)
		_, _ = buf.Write(specifications.SPACE)
		_, _ = buf.WriteString(ctx.FormatIdent(fmt.Sprintf("%s_%s", column.Name, query)))
		break
	default:
		err = errors.Warning("sql: render virtual field failed").
			WithCause(fmt.Errorf("kind of %s is not valid virtual", column.Field)).
			WithMeta("table", spec.Key).
			WithMeta("field", column.Field)
		return
	}

	fragment = buf.String()
	return
}
