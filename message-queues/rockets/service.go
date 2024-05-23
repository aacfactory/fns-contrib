package rockets

import "github.com/aacfactory/fns/services"

var (
	endpointName = []byte("rocketmq")
)

type service struct {
	services.Abstract
}

func (svc *service) Construct(options services.Options) (err error) {

	return
}
