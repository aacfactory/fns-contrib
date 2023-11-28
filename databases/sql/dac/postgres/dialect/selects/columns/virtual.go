package columns

import (
	"fmt"
	"github.com/aacfactory/errors"
	"github.com/aacfactory/fns-contrib/databases/sql/dac/specifications"
	"github.com/valyala/bytebufferpool"
)

// Virtual
// ({query}) AS {name}
func Virtual(ctx specifications.Context, spec *specifications.Specification, column *specifications.Column) (fragment []byte, err error) {
	buf := bytebufferpool.Get()
	defer bytebufferpool.Put(buf)

	query, ok := column.Virtual()
	if !ok {
		err = errors.Warning("sql: render virtual field failed").
			WithCause(fmt.Errorf("%s is not virtual", column.Field)).
			WithMeta("table", spec.Key).
			WithMeta("field", column.Field)
		return
	}

	_, _ = buf.Write(specifications.LB)
	_, _ = buf.Write([]byte(query))
	_, _ = buf.Write(specifications.RB)
	_, _ = buf.Write(specifications.SPACE)
	_, _ = buf.Write(specifications.AS)
	_, _ = buf.Write(specifications.SPACE)
	_, _ = buf.Write(ctx.FormatIdent([]byte(column.Name)))

	return
}
