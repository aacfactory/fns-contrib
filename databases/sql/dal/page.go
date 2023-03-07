package dal

import (
	"context"
	"fmt"
	"github.com/aacfactory/errors"
	"math"
)

// Pager
// @title 页
// @description 分页查询结果
type Pager[T any] struct {
	// No
	// @title 页码
	// @description 当前页码
	No int64 `json:"no"`
	// Num
	// @title 总页数
	// @description 总页数
	Num int64 `json:"num"`
	// Total
	// @title 总页内容数
	// @description 总页内容数
	Total int64 `json:"total"`
	// Items
	// @title 页内容
	// @description 页内容列表
	Items []T `json:"items"`
}

func Page[T Model](ctx context.Context, conditions *Conditions, orders *Orders, page *PageRequest) (result *Pager[T], err errors.CodeError) {
	if page == nil {
		err = errors.Warning("dal: query page failed").WithCause(fmt.Errorf("pager is required"))
		return
	}
	results, queryErr := QueryWithRange[T](ctx, conditions, orders, page.MapToRange())
	if queryErr != nil {
		err = errors.ServiceError("dal: query page failed").WithCause(queryErr)
		return
	}
	if results == nil || len(results) == 0 {
		result = &Pager[T]{
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
	result = &Pager[T]{
		Items: results,
		No:    int64(page.no),
		Num:   int64(math.Ceil(float64(count) / float64(page.size))),
		Total: count,
	}
	return
}
