package postgres

import (
	"github.com/aacfactory/fns-contrib/databases/sql/dac/specifications"
	"io"
)

type Generic struct {
	spec                     *specifications.Specification
	inserts                  []byte
	insertValues             []int
	insertOrUpdates          []byte
	insertOrUpdateValues     []int
	insertWhenExists         []byte
	insertWhenExistValues    []int
	insertWhenNotExists      []byte
	insertWhenNotExistValues []int
	updates                  []byte
	updateValues             []int
	deletes                  []byte
	deleteValues             []int
	counts                   []byte
	countValues              []int
	exists                   []byte
	existValues              []int
	selects                  []byte
	selectValues             []int
}

func (generic *Generic) Render(ctx specifications.Context, w io.Writer, instance specifications.Table) (err error) {

	return
}
