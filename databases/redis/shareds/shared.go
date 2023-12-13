package shareds

import (
	"github.com/aacfactory/errors"
	"github.com/aacfactory/fns-contrib/databases/redis/configs"
	"github.com/aacfactory/fns/shareds"
	"github.com/redis/rueidis"
)

func Shared(options ...configs.Option) (v shareds.Shared) {
	opt := configs.Options{}
	for _, option := range options {
		option(&opt)
	}
	v = &shared{
		options: opt,
	}
	return
}

func NewWithClient(client rueidis.Client) (v shareds.Shared, err error) {

	lockers, lockersErr := NewLockersWithClient(client)
	if lockersErr != nil {
		err = errors.Warning("shared: new failed").WithMeta("shareds", "redis").WithCause(lockersErr)
		return
	}
	store, storeErr := NewStoreWithClient(client)
	if storeErr != nil {
		err = errors.Warning("shared: new failed").WithMeta("shareds", "redis").WithCause(storeErr)
		return
	}

	v = &shared{
		client:  client,
		lockers: lockers,
		store:   store,
		shared:  true,
	}

	return
}

type shared struct {
	options configs.Options
	client  rueidis.Client
	lockers shareds.Lockers
	store   shareds.Store
	shared  bool
}

func (s *shared) Construct(options shareds.Options) (err error) {
	config := configs.Config{}
	configErr := options.Config.As(&config)
	if configErr != nil {
		err = errors.Warning("shared: construct failed").WithMeta("shareds", "redis").WithCause(configErr)
		return
	}
	client, clientErr := config.Make(s.options)
	if clientErr != nil {
		err = errors.Warning("shared: construct failed").WithMeta("shareds", "redis").WithCause(clientErr)
		return
	}
	lockers, lockersErr := NewLockersWithClient(client)
	if lockersErr != nil {
		err = errors.Warning("shared: construct failed").WithMeta("shareds", "redis").WithCause(lockersErr)
		return
	}
	store, storeErr := NewStoreWithClient(client)
	if storeErr != nil {
		err = errors.Warning("shared: construct failed").WithMeta("shareds", "redis").WithCause(storeErr)
		return
	}
	s.client = client
	s.lockers = lockers
	s.store = store
	return
}

func (s *shared) Lockers() (lockers shareds.Lockers) {
	lockers = s.lockers
	return
}

func (s *shared) Store() (store shareds.Store) {
	store = s.store
	return
}

func (s *shared) Close() {
	if s.shared {
		return
	}
	s.client.Close()
}
