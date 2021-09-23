package redis

import (
	"github.com/aacfactory/errors"
	"github.com/aacfactory/fns"
	"github.com/aacfactory/json"
	"time"
)

func GetBySingleFlight(ctx fns.Context, key string, timeout time.Duration, fn func() (result json.RawMessage, err errors.CodeError)) (result json.RawMessage, err errors.CodeError) {
	// todo
	return
}
