package dac

import (
	"github.com/aacfactory/errors"
	"github.com/aacfactory/fns-contrib/databases/sql/dac/specifications"
	"github.com/aacfactory/fns/context"
	"math"
)

type Pager[T Table] struct {
	// No
	// @title no
	// @description no of page
	No int `json:"no"`
	// Pages
	// @title pages
	// @description total pages
	Pages int64 `json:"pages"`
	// Total
	// @title total
	// @description total entries
	Total int64 `json:"total"`
	// Entries
	// @title entries
	// @description entries of page
	Entries []T `json:"entries"`
}

func Page[T Table](ctx context.Context, no int, size int, options ...QueryOption) (page Pager[T], err error) {
	opt := QueryOptions{}
	for _, option := range options {
		option(&opt)
	}

	count, countErr := Count[T](ctx, opt.cond)
	if countErr != nil {
		err = errors.Warning("sql: page failed").WithCause(countErr)
		return
	}
	if count == 0 {
		page = Pager[T]{
			No:      0,
			Pages:   0,
			Total:   0,
			Entries: nil,
		}
		return
	}

	rng := specifications.PG(no, size).Range()
	entries, queryErr := Query[T](ctx, rng.Offset, rng.Length, options...)
	if queryErr != nil {
		err = errors.Warning("sql: page failed").WithCause(queryErr)
		return
	}

	page = Pager[T]{
		No:      no,
		Pages:   int64(math.Ceil(float64(count) / float64(size))),
		Total:   count,
		Entries: entries,
	}
	return
}
