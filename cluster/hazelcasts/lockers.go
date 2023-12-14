package hazelcasts

import (
	"fmt"
	"github.com/aacfactory/errors"
	"github.com/aacfactory/fns/context"
	"github.com/aacfactory/fns/shareds"
	"github.com/hazelcast/hazelcast-go-client"
	"time"
)

type Locker struct {
	value *Maps
	key   []byte
	ttl   time.Duration
}

func (locker *Locker) Lock(ctx context.Context) (err error) {
	if locker.ttl == 0 {
		err = locker.value.Lock(ctx, locker.key)
	} else {
		err = locker.value.LockWithLease(ctx, locker.key, locker.ttl)
	}
	if err != nil {
		err = errors.Warning("hazelcast: lock failed").WithCause(err)
		return
	}
	return
}

func (locker *Locker) Unlock(ctx context.Context) (err error) {
	err = locker.value.Unlock(ctx, locker.key)
	if err != nil {
		err = errors.Warning("hazelcast: unlock failed").WithCause(err)
		return
	}
	return
}

func NewLockers(ctx context.Context, client *hazelcast.Client, size int) (v shareds.Lockers, err error) {
	value, valueErr := NewMaps(ctx, "fns:shared:lockers", client, size)
	if valueErr != nil {
		err = errors.Warning("hazelcast: new shared lockers failed").WithCause(valueErr)
		return
	}
	v = &Lockers{
		value: value,
	}
	return
}

type Lockers struct {
	value *Maps
}

func (lockers *Lockers) Acquire(_ context.Context, key []byte, ttl time.Duration) (locker shareds.Locker, err error) {
	if len(key) == 0 {
		err = errors.Warning("hazelcast: acquire locker failed").WithCause(fmt.Errorf("key is required"))
	}
	locker = &Locker{
		value: lockers.value,
		key:   key,
		ttl:   ttl,
	}
	return
}

func (lockers *Lockers) Close() {
}
