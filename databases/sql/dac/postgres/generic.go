package postgres

import (
	"github.com/aacfactory/fns-contrib/databases/sql/dac/specifications"
	"io"
)

type Generic struct {
	spec         *specifications.Specification
	selects      []byte
	selectValues []int
	inserts      []byte
	insertValues []int
	updates      []byte
	updateValues []int
	deletes      []byte
	deleteValues []int
}

func (generic *Generic) Render(ctx specifications.Context, w io.Writer, instance specifications.Table) (err error) {

	return
}
