package redis

import (
	"github.com/aacfactory/fns/services"
)

var (
	endpointName = []byte("redis")
)

func New(databases ...string) services.Service {

	return &_service_{
		Abstract: services.NewAbstract(string(endpointName), true),
	}
}

type Service struct {
	services.Abstract
}

type _service_ struct {
	services.Abstract
	defaultDatabaseName string
}
