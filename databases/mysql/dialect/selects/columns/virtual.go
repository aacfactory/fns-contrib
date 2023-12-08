package columns

import (
	"fmt"
	"github.com/aacfactory/errors"
	"github.com/aacfactory/fns-contrib/databases/sql/dac/specifications"
	"github.com/valyala/bytebufferpool"
)

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
	case specifications.BasicVirtualQuery, specifications.ObjectVirtualQuery, specifications.ArrayVirtualQuery:
		_, _ = buf.Write(specifications.LB)
		_, _ = buf.Write([]byte(query))
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
