package sql

import (
	"github.com/aacfactory/errors"
	"github.com/aacfactory/fns"
	"math"
)

type Paged struct {
	No    int
	Num   int
	Total int
}

func (d *dao) Page(ctx fns.Context, param *QueryParam) (page Paged, err errors.CodeError) {
	has, queryErr := d.Query(ctx, param)
	if queryErr != nil {
		err = errors.ServiceError("fns SQL: use DAO failed for Page()").WithCause(queryErr)
		return
	}
	if has {
		count, countErr := d.Count(ctx, param)
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
