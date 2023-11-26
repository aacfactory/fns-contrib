package dac

import (
	"github.com/aacfactory/fns-contrib/databases/sql/dac/conditions"
	"github.com/aacfactory/fns-contrib/databases/sql/dac/models"
	"github.com/aacfactory/fns/context"
)

func Count[T models.Table](ctx context.Context, cond conditions.Condition) (count int64, err error) {

	return
}
