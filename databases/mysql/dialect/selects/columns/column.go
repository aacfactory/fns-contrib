package columns

import (
	"github.com/aacfactory/fns-contrib/databases/sql/dac/specifications"
)

func Fragment(ctx specifications.Context, spec *specifications.Specification, column *specifications.Column) (fragment string, err error) {
	switch column.Kind {
	case specifications.Reference:
		fragment, err = Reference(ctx, spec, column)
		break
	case specifications.Link:
		fragment, err = Link(ctx, spec, column)
		break
	case specifications.Links:
		fragment, err = Links(ctx, spec, column)
		break
	case specifications.Virtual:
		fragment, err = Virtual(ctx, spec, column)
		break
	default:
		fragment = ctx.FormatIdent(column.Name)
		break
	}
	return
}
