package mysql

import (
	"context"
	"github.com/aacfactory/errors"
	"math"
	"reflect"
)

type PageInfo struct {
	No    int
	Num   int
	Total int
}

func (p *PageInfo) Empty() bool {
	return p.Total == 0
}

func Page(ctx context.Context, cond *Conditions, orders *Orders, pageNo int, pageSize int, rows interface{}) (page *PageInfo, err errors.CodeError) {
	if pageNo < 1 {
		pageNo = 1
	}
	if pageSize < 1 {
		pageSize = 10
	}
	rng := NewRange((pageNo-1)*pageSize, pageSize)
	fetched, fetchErr := query0(ctx, cond, orders, rng, rows)
	if fetchErr != nil {
		err = errors.ServiceError("mysql: page failed").WithCause(fetchErr).WithMeta("mysql", "page")
		return
	}
	if !fetched {
		page = &PageInfo{
			No:    pageNo,
			Num:   0,
			Total: 0,
		}
		return
	}
	// count
	tab := reflect.New(reflect.TypeOf(rows).Elem().Elem().Elem()).Interface().(Table)
	count, countErr := Count(ctx, cond, tab)
	if countErr != nil {
		err = errors.ServiceError("mysql: page failed").WithCause(countErr).WithMeta("mysql", "page")
		return
	}
	if count == 0 {
		page = &PageInfo{
			No:    pageNo,
			Num:   0,
			Total: 0,
		}
		return
	}
	page = &PageInfo{
		No:    pageNo,
		Num:   int(math.Ceil(float64(count) / float64(pageSize))),
		Total: count,
	}
	return
}
