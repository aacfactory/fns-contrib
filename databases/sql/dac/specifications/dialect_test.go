package specifications_test

import (
	"fmt"
	"github.com/aacfactory/fns-contrib/databases/sql/dac/specifications"
)

type QueryPlaceholder struct {
	count int
}

func (q *QueryPlaceholder) Next() (v []byte) {
	q.count++
	return []byte(fmt.Sprintf("$%d", q.count))
}

type Dialect struct {
}

func (d *Dialect) Name() string {
	return "tests"
}

func (d *Dialect) FormatIdent(ident []byte) []byte {
	if ident[0] == '"' {
		return ident
	}
	p := make([]byte, len(ident)+2)
	p[0] = '"'
	p[len(ident)+1] = '"'
	copy(p[1:], ident)
	return p[:]
}

func (d *Dialect) QueryPlaceholder() specifications.QueryPlaceholder {
	return &QueryPlaceholder{}
}

func (d *Dialect) Insert(ctx specifications.Context, spec *specifications.Specification) (method specifications.Method, query []byte, arguments []any, err error) {
	return
}

func (d *Dialect) InsertOrUpdate(ctx specifications.Context, spec *specifications.Specification) (method specifications.Method, query []byte, arguments []any, err error) {
	return
}

func (d *Dialect) InsertWhenExist(ctx specifications.Context, spec *specifications.Specification, source string) (method specifications.Method, query []byte, arguments []any, err error) {
	return
}

func (d *Dialect) InsertWhenNotExist(ctx specifications.Context, spec *specifications.Specification, source string) (method specifications.Method, query []byte, arguments []any, err error) {
	return
}

func (d *Dialect) Update(ctx specifications.Context, spec *specifications.Specification) (method specifications.Method, query []byte, arguments []any, err error) {
	return
}

func (d *Dialect) UpdateFields(ctx specifications.Context, spec *specifications.Specification, fields []specifications.FieldValue, cond specifications.Condition) (method specifications.Method, query []byte, arguments []any, err error) {

	return
}

func (d *Dialect) Delete(ctx specifications.Context, spec *specifications.Specification) (method specifications.Method, query []byte, arguments []any, err error) {

	return
}

func (d *Dialect) DeleteWithConditions(ctx specifications.Context, spec *specifications.Specification, cond specifications.Condition) (method specifications.Method, query []byte, arguments []any, err error) {

	return
}

func (d *Dialect) Exist(ctx specifications.Context, spec *specifications.Specification, cond specifications.Condition) (method specifications.Method, query []byte, arguments []any, err error) {

	return
}

func (d *Dialect) Count(ctx specifications.Context, spec *specifications.Specification, cond specifications.Condition) (method specifications.Method, query []byte, arguments []any, err error) {

	return
}

func (d *Dialect) Select(ctx specifications.Context, spec *specifications.Specification, cond specifications.Condition, orders specifications.Orders, groupBy specifications.GroupBy, having specifications.Having, offset int, length int) (method specifications.Method, query []byte, arguments []any, err error) {

	return
}
