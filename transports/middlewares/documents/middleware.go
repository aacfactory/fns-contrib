package documents

import "github.com/aacfactory/fns/transports"

func New() transports.Middleware {
	return &middleware{}
}

type middleware struct {
}

func (middle *middleware) Name() string {
	//TODO implement me
	panic("implement me")
}

func (middle *middleware) Construct(options transports.MiddlewareOptions) error {
	//TODO implement me
	panic("implement me")
}

func (middle *middleware) Handler(next transports.Handler) transports.Handler {
	//TODO implement me
	panic("implement me")
}

func (middle *middleware) Close() {
	//TODO implement me
	panic("implement me")
}
