package hazelcasts

import (
	"github.com/aacfactory/errors"
	"github.com/aacfactory/fns/commons/bytex"
	"github.com/aacfactory/fns/context"
	"github.com/aacfactory/fns/shareds"
	"github.com/hazelcast/hazelcast-go-client"
	"time"
)

func NewStore(ctx context.Context, client *hazelcast.Client) (v shareds.Store, err error) {
	value, valueErr := client.GetMap(ctx, "fns:shared:store")
	if valueErr != nil {
		err = errors.Warning("hazelcast: new shared store failed").WithCause(valueErr)
		return
	}
	v = &Store{
		client:           client,
		value:            value,
		counterKeyPrefix: []byte("fns:shared:store:"),
	}
	return
}

type Store struct {
	client           *hazelcast.Client
	value            *hazelcast.Map
	counterKeyPrefix []byte
}

func (store *Store) Get(ctx context.Context, key []byte) (value []byte, has bool, err error) {
	v, getErr := store.value.Get(ctx, bytex.ToString(key))
	if getErr != nil {
		err = errors.Warning("hazelcast: shared store get failed").WithCause(getErr)
		return
	}
	if v == nil {
		return
	}
	value, has = v.([]byte)
	return
}

func (store *Store) Set(ctx context.Context, key []byte, value []byte) (err error) {
	err = store.value.Set(ctx, bytex.ToString(key), value)
	if err != nil {
		err = errors.Warning("hazelcast: shared store set failed").WithCause(err)
		return
	}
	return
}

func (store *Store) SetWithTTL(ctx context.Context, key []byte, value []byte, ttl time.Duration) (err error) {
	err = store.value.SetWithTTL(ctx, bytex.ToString(key), value, ttl)
	if err != nil {
		err = errors.Warning("hazelcast: shared store set failed").WithCause(err)
		return
	}
	return
}

func (store *Store) Incr(ctx context.Context, key []byte, delta int64) (v int64, err error) {
	counter, counterErr := store.client.GetPNCounter(ctx, bytex.ToString(append(store.counterKeyPrefix, key...)))
	if counterErr != nil {
		err = errors.Warning("hazelcast: shared store incr failed").WithCause(counterErr)
		return
	}
	if delta == 0 {
		v, err = counter.Get(ctx)
		if err != nil {
			err = errors.Warning("hazelcast: shared store incr failed").WithCause(err)
			return
		}
		return
	}
	incr := delta > 0
	if incr {
		for i := int64(0); i < delta; i++ {
			v, err = counter.IncrementAndGet(ctx)
			if err != nil {
				err = errors.Warning("hazelcast: shared store incr failed").WithCause(err)
				return
			}
		}
	} else {
		delta = delta * -1
		for i := int64(0); i < delta; i++ {
			v, err = counter.DecrementAndGet(ctx)
			if err != nil {
				err = errors.Warning("hazelcast: shared store incr failed").WithCause(err)
				return
			}
		}
	}
	return
}

func (store *Store) Remove(ctx context.Context, key []byte) (err error) {
	_, err = store.value.Remove(ctx, bytex.ToString(key))
	if err != nil {
		err = errors.Warning("hazelcast: shared store remove failed").WithCause(err)
		return
	}
	return
}

func (store *Store) Close() {
}
