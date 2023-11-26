package dac

import (
	"github.com/aacfactory/fns-contrib/databases/sql/dac/models"
	"github.com/aacfactory/fns/context"
)

type Pager[T models.Table] struct {
	No      int64 `json:"no"`
	Num     int64 `json:"num"`
	Total   int64 `json:"total"`
	Entries []T   `json:"entries"`
}

func Page[T models.Table](ctx context.Context, no int, size int, options ...Option) (page Pager[T], err error) {

	return
}
