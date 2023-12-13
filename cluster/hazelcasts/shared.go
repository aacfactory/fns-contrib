package hazelcasts

import (
	"github.com/aacfactory/configures"
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
		shared.lockers, err = NewLockers(context.TODO(), shared.client)
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
		shared.store, err = NewStore(context.TODO(), shared.client)
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
