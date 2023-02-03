package dal

import (
	"context"
	"github.com/aacfactory/errors"
)

type PageData[T Model] struct {
	Items []T
	No    int64
	Num   int64
	Total int64
}

func Page[T Model](ctx context.Context, conditions *Conditions, orders *Orders, pager *Pager) (result *PageData[T], err errors.CodeError) {
	//m := new(T)
	return
}
