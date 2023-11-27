package dac

import (
	"github.com/aacfactory/fns/context"
)

type Pager[T Table] struct {
	No      int64 `json:"no"`
	Num     int64 `json:"num"`
	Total   int64 `json:"total"`
	Entries []T   `json:"entries"`
}

func Page[T Table](ctx context.Context, no int, size int, options ...QueryOption) (page Pager[T], err error) {

	return
}
