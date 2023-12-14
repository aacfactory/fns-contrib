package hazelcasts

import (
	"fmt"
	"github.com/aacfactory/errors"
	"github.com/aacfactory/fns/commons/bytex"
	"github.com/aacfactory/fns/context"
	"github.com/cespare/xxhash/v2"
	"github.com/hazelcast/hazelcast-go-client"
	"time"
)

func NewMaps(ctx context.Context, name string, client *hazelcast.Client, size int) (v *Maps, err error) {
	if size < 1 {
		size = 64
	}
	v = &Maps{
		values: make([]*hazelcast.Map, size),
		size:   uint64(size),
	}
	for i := 0; i < size; i++ {
		key := fmt.Sprintf("%s:%d", name, i+1)
		m, mErr := client.GetMap(ctx, key)
		if mErr != nil {
			err = errors.Warning("hazelcast: new maps failed").WithCause(mErr)
			return
		}
		v.values[i] = m
	}
	return
}

type Maps struct {
	values []*hazelcast.Map
	size   uint64
}

func (mm *Maps) Get(ctx context.Context, key []byte) (value []byte, has bool, err error) {
	idx := xxhash.Sum64(key) % mm.size
	m := mm.values[idx]
	v, getErr := m.Get(ctx, bytex.ToString(key))
	if getErr != nil {
		err = getErr
		return
	}
	if v == nil {
		return
	}
	value, has = v.([]byte)
	return
}

func (mm *Maps) Set(ctx context.Context, key []byte, value []byte) (err error) {
	idx := xxhash.Sum64(key) % mm.size
	m := mm.values[idx]
	err = m.Set(ctx, bytex.ToString(key), value)
	return
}

func (mm *Maps) SetWithTTL(ctx context.Context, key []byte, value []byte, ttl time.Duration) (err error) {
	idx := xxhash.Sum64(key) % mm.size
	m := mm.values[idx]
	err = m.SetWithTTL(ctx, bytex.ToString(key), value, ttl)
	return
}

func (mm *Maps) Remove(ctx context.Context, key []byte) (err error) {
	idx := xxhash.Sum64(key) % mm.size
	m := mm.values[idx]
	err = m.Delete(ctx, bytex.ToString(key))
	return
}

func (mm *Maps) SetTTL(ctx context.Context, key []byte, ttl time.Duration) (err error) {
	idx := xxhash.Sum64(key) % mm.size
	m := mm.values[idx]
	err = m.SetTTL(ctx, bytex.ToString(key), ttl)
	return
}

func (mm *Maps) Lock(ctx context.Context, key []byte) (err error) {
	idx := xxhash.Sum64(key) % mm.size
	m := mm.values[idx]
	err = m.Lock(ctx, bytex.ToString(key))
	return
}

func (mm *Maps) LockWithLease(ctx context.Context, key []byte, ttl time.Duration) (err error) {
	idx := xxhash.Sum64(key) % mm.size
	m := mm.values[idx]
	err = m.LockWithLease(ctx, bytex.ToString(key), ttl)
	return
}

func (mm *Maps) Unlock(ctx context.Context, key []byte) (err error) {
	idx := xxhash.Sum64(key) % mm.size
	m := mm.values[idx]
	err = m.Unlock(ctx, bytex.ToString(key))
	return
}

func (mm *Maps) NewLockContext(ctx context.Context, key []byte) context.Context {
	idx := xxhash.Sum64(key) % mm.size
	m := mm.values[idx]
	return context.Wrap(m.NewLockContext(ctx))
}
