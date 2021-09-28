package sql

import (
	"github.com/aacfactory/errors"
	"github.com/aacfactory/fns"
	"math"
	"reflect"
)

type Paged struct {
	No    int
	Num   int
	Total int
}

func (d *dao) Page(ctx fns.Context, param *QueryParam, rows interface{}) (page Paged, err errors.CodeError) {
	has, queryErr := d.Query(ctx, param, rows)
	if queryErr != nil {
		err = errors.ServiceError("fns SQL: use DAO failed for Page()").WithCause(queryErr)
		return
	}
	if has {
		rev := reflect.ValueOf(rows).Elem().Index(0).Interface()
		row := rev.(TableRow)
		count, countErr := d.Count(ctx, param, row)
		if countErr != nil {
			err = errors.ServiceError("fns SQL: use DAO failed for Page()").WithCause(countErr)
			return
		}
		page.No = param.pageNo
		page.Num = int(math.Ceil(float64(count) / float64(param.pageSize)))
		page.Total = count
	}
	return
}
