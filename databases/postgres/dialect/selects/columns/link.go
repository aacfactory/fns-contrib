package columns

import (
	"github.com/aacfactory/errors"
	"github.com/aacfactory/fns-contrib/databases/sql/dac/specifications"
)

// Link
// same as Reference
func Link(ctx specifications.Context, spec *specifications.Specification, column *specifications.Column) (fragment []byte, err error) {
	fragment, err = Reference(ctx, spec, column)
	if err != nil {
		err = errors.Warning("sql: render link field failed").
			WithCause(err).
			WithMeta("table", spec.Key).
			WithMeta("field", column.Field)
		return
	}
	return
}
