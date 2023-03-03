package dal

import (
	"context"
	"fmt"
	"github.com/aacfactory/errors"
	"math"
)

type PageResult[T Model] struct {
	Items []T
	No    int64
	Num   int64
	Total int64
}

func Page[T Model](ctx context.Context, conditions *Conditions, orders *Orders, pager *Pager) (result *PageResult[T], err errors.CodeError) {
	if pager == nil {
		err = errors.Warning("dal: query page failed").WithCause(fmt.Errorf("pager is required"))
		return
	}
	results, queryErr := QueryWithRange[T](ctx, conditions, orders, pager.MapToRange())
	if queryErr != nil {
		err = errors.ServiceError("dal: query page failed").WithCause(queryErr)
		return
	}
	if results == nil || len(results) == 0 {
		result = &PageResult[T]{
			Items: results,
			No:    0,
			Num:   0,
			Total: 0,
		}
	}
	count, countErr := Count[T](ctx, conditions)
	if countErr != nil {
		err = errors.ServiceError("dal: query page failed").WithCause(countErr)
		return
	}
	result = &PageResult[T]{
		Items: results,
		No:    int64(pager.no),
		Num:   int64(math.Ceil(float64(count) / float64(pager.size))),
		Total: count,
	}
	return
}
