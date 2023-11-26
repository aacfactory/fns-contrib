package dac

import (
	"github.com/aacfactory/fns-contrib/databases/sql/dac/conditions"
	"github.com/aacfactory/fns-contrib/databases/sql/dac/models"
	"github.com/aacfactory/fns/context"
)

func Exist[T models.Table](ctx context.Context, cond conditions.Condition) (has bool, err error) {

	return
}
