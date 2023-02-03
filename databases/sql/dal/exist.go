package dal

import (
	"context"
	"github.com/aacfactory/errors"
)

func Exist[T Model](ctx context.Context, conditions *Conditions) (has bool, err errors.CodeError) {
	//m := new(T)
	return
}
