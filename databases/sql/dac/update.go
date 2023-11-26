package dac

import (
	"github.com/aacfactory/fns-contrib/databases/sql/dac/conditions"
	"github.com/aacfactory/fns-contrib/databases/sql/dac/models"
	"github.com/aacfactory/fns-contrib/databases/sql/dac/updates"
	"github.com/aacfactory/fns/context"
)

func Update[T models.Table](ctx context.Context, entry T) (err error) {

	return
}

func UpdateField[T models.Table](ctx context.Context, fields updates.Fields, cond conditions.Condition) (err error) {

	return
}
