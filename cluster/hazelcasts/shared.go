package hazelcasts

import (
	"github.com/aacfactory/configures"
	"github.com/aacfactory/errors"
	"github.com/aacfactory/fns-contrib/cluster/hazelcasts/configs"
	"github.com/aacfactory/fns/context"
	"github.com/aacfactory/fns/shareds"
	"github.com/hazelcast/hazelcast-go-client"
)

func NewShared(client *hazelcast.Client) shareds.Shared {
	return &Shared{
		client:  client,
		lockers: nil,
		store:   nil,
	}
}

type Shared struct {
	client  *hazelcast.Client
	lockers shareds.Lockers
	store   shareds.Store
}

func (shared *Shared) Construct(options shareds.Options) (err error) {
	if extraSharedLockersBuilder != nil {
		node, has := options.Config.Node("lockers")
		if !has {
			node, _ = configures.NewJsonConfig([]byte{'{', '}'})
		}
		shared.lockers, err = extraSharedLockersBuilder.Build(context.TODO(), node)
		if err != nil {
			return
		}
	} else {
		config := configs.SharedConfig{}
		configErr := options.Config.As(&config)
		if configErr != nil {
			err = errors.Warning("hazelcast: construct shared failed").WithCause(configErr)
			return
		}
		shared.lockers, err = NewLockers(context.TODO(), shared.client, config.LockersSize)
	}
	if extraSharedStoreBuilder != nil {
		node, has := options.Config.Node("store")
		if !has {
			node, _ = configures.NewJsonConfig([]byte{'{', '}'})
		}
		shared.store, err = extraSharedStoreBuilder.Build(context.TODO(), node)
		if err != nil {
			return
		}
	} else {
		config := configs.SharedConfig{}
		configErr := options.Config.As(&config)
		if configErr != nil {
			err = errors.Warning("hazelcast: construct shared failed").WithCause(configErr)
			return
		}
		shared.store, err = NewStore(context.TODO(), shared.client, config.StoreSize)
	}
	return
}

func (shared *Shared) Lockers() (lockers shareds.Lockers) {
	lockers = shared.lockers
	return
}

func (shared *Shared) Store() (store shareds.Store) {
	store = shared.store
	return
}

func (shared *Shared) Close() {
}
