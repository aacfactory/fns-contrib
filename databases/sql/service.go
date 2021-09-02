package sql

import (
	"github.com/aacfactory/configuares"
	"github.com/aacfactory/errors"
	"github.com/aacfactory/fns"
)

type Service struct {

}

func (svc *Service) Namespace() (namespace string) {
	panic("implement me")
}

func (svc *Service) Internal() (internal bool) {
	panic("implement me")
}

func (svc *Service) Build(config configuares.Config) (err error) {
	panic("implement me")
}

func (svc *Service) Description() (description []byte) {
	panic("implement me")
}

func (svc *Service) Handle(ctx fns.Context, fn string, argument fns.Argument) (result interface{}, err errors.CodeError) {
	panic("implement me")
}

func (svc *Service) Close() (err error) {
	panic("implement me")
}

