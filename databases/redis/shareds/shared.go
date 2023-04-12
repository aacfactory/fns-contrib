package shareds

import (
	"github.com/aacfactory/fns/service"
	"github.com/aacfactory/fns/service/shareds"
	"time"
)

type Options struct {
	DefaultCacheTTL time.Duration `json:"defaultCacheTTL"`
}

func Shared(options Options) service.Shared {
	return &shared_{
		store:   Store(),
		lockers: Lockers(),
		cache:   Cache(options.DefaultCacheTTL),
	}
}

type shared_ struct {
	store   shareds.Store
	lockers shareds.Lockers
	cache   shareds.Caches
}

func (s *shared_) Lockers() (lockers shareds.Lockers) {
	lockers = s.lockers
	return
}

func (s *shared_) Store() (v shareds.Store) {
	v = s.store
	return
}

func (s *shared_) Caches() (lockers shareds.Caches) {
	lockers = s.cache
	return
}
