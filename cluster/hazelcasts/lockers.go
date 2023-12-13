package hazelcasts

import (
	"fmt"
	"github.com/aacfactory/errors"
	"github.com/aacfactory/fns/commons/bytex"
	"github.com/aacfactory/fns/context"
	"github.com/aacfactory/fns/shareds"
	"github.com/hazelcast/hazelcast-go-client"
	"time"
)

type Locker struct {
	value *hazelcast.Map
	key   string
	ttl   time.Duration
}

func (locker *Locker) Lock(ctx context.Context) (err error) {
	if locker.ttl == 0 {
		err = locker.value.Lock(locker.value.NewLockContext(ctx), locker.key)
	} else {
		err = locker.value.LockWithLease(locker.value.NewLockContext(ctx), locker.key, locker.ttl)
	}
	if err != nil {
		err = errors.Warning("hazelcast: lock failed").WithCause(err)
		return
	}
	return
}

func (locker *Locker) Unlock(ctx context.Context) (err error) {
	err = locker.value.Unlock(locker.value.NewLockContext(ctx), locker.key)
	if err != nil {
		err = errors.Warning("hazelcast: unlock failed").WithCause(err)
		return
	}
	return
}

func NewLockers(ctx context.Context, client *hazelcast.Client) (v shareds.Lockers, err error) {
	value, valueErr := client.GetMap(ctx, "fns:shared:lockers")
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
	value *hazelcast.Map
}

func (lockers *Lockers) Acquire(_ context.Context, key []byte, ttl time.Duration) (locker shareds.Locker, err error) {
	if len(key) == 0 {
		err = errors.Warning("hazelcast: acquire locker failed").WithCause(fmt.Errorf("key is required"))
	}
	locker = &Locker{
		value: lockers.value,
		key:   bytex.ToString(key),
		ttl:   ttl,
	}
	return
}

func (lockers *Lockers) Close() {
}
