package limiter

import (
	"github.com/aacfactory/configures"
	"github.com/aacfactory/fns/context"
	"github.com/aacfactory/logs"
)

type AlarmOptions struct {
	Log    logs.Logger
	Config configures.Config
}

type Alarm interface {
	Construct(options AlarmOptions) (err error)
	Handle(ctx context.Context) (err error)
	Shutdown()
}
