package hazelcasts

import (
	"context"
	"fmt"
	"github.com/aacfactory/errors"
	"github.com/aacfactory/fns/commons/bytex"
	"github.com/cespare/xxhash/v2"
	"github.com/hazelcast/hazelcast-go-client"
	"time"
)

func NewMaps(ctx context.Context, name string, client *hazelcast.Client, size int) (v Maps, err error) {
	if size < 1 {
		size = 64
	}
	for i := 0; i < size; i++ {
		key := fmt.Sprintf("%s:%d", name, i+1)
		m, mErr := client.GetMap(ctx, key)
		if mErr != nil {
			err = errors.Warning("hazelcast: new maps failed").WithCause(mErr)
			return
		}
		v = append(v, m)
	}
	return
}

type Maps []*hazelcast.Map

func (mm Maps) Get(ctx context.Context, key []byte) (value []byte, has bool, err error) {
	idx := xxhash.Sum64(key) % uint64(len(mm))
	m := mm[idx]
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

func (mm Maps) Set(ctx context.Context, key []byte, value []byte) (err error) {
	idx := xxhash.Sum64(key) % uint64(len(mm))
	m := mm[idx]
	err = m.Set(ctx, bytex.ToString(key), value)
	return
}

func (mm Maps) SetWithTTL(ctx context.Context, key []byte, value []byte, ttl time.Duration) (err error) {
	idx := xxhash.Sum64(key) % uint64(len(mm))
	m := mm[idx]
	err = m.SetWithTTL(ctx, bytex.ToString(key), value, ttl)
	return
}

func (mm Maps) Remove(ctx context.Context, key []byte) (err error) {
	idx := xxhash.Sum64(key) % uint64(len(mm))
	m := mm[idx]
	err = m.Delete(ctx, bytex.ToString(key))
	return
}
