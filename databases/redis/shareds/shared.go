package shareds

import (
	"github.com/aacfactory/fns/service"
	"github.com/aacfactory/fns/service/shared"
)

func Shared() service.Shared {
	return &shared_{
		store:   Store(),
		lockers: Lockers(),
	}
}

type shared_ struct {
	store   shared.Store
	lockers shared.Lockers
}

func (s *shared_) Lockers() (lockers shared.Lockers) {
	lockers = s.lockers
	return
}

func (s *shared_) Store() (v shared.Store) {
	v = s.store
	return
}
