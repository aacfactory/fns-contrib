package hazelcasts

import (
	"fmt"
	"github.com/aacfactory/errors"
	"github.com/aacfactory/fns/barriers"
	"github.com/aacfactory/fns/commons/bytex"
	"github.com/aacfactory/fns/commons/objects"
	"github.com/aacfactory/fns/context"
	"github.com/aacfactory/json"
	"github.com/hazelcast/hazelcast-go-client"
	"golang.org/x/sync/singleflight"
	"time"
)

func NewBarrier(ctx context.Context, client *hazelcast.Client) (v barriers.Barrier, err error) {
	mm, getErr := client.GetMap(ctx, "fns:barrier")
	if getErr != nil {
		err = errors.Warning("hazelcast: new barrier failed").WithCause(getErr)
		return
	}
	ttl := 5 * time.Second
	interval := 50 * time.Millisecond
	loops := int(ttl / interval)

	v = &Barrier{
		values:   mm,
		ttl:      ttl,
		interval: interval,
		loops:    loops,
		group:    singleflight.Group{},
	}
	return
}

type Barrier struct {
	values   *hazelcast.Map
	ttl      time.Duration
	interval time.Duration
	loops    int
	group    singleflight.Group
}

func (barrier *Barrier) Do(ctx context.Context, key []byte, fn func() (result interface{}, err error)) (result barriers.Result, err error) {
	if len(key) == 0 {
		key = []byte{'-'}
	}
	r, doErr, _ := barrier.group.Do(bytex.ToString(key), func() (r interface{}, err error) {
		r, err = barrier.doRemote(ctx, key, fn)
		return
	})
	if doErr != nil {
		err = doErr
		return
	}
	result = objects.New(r)
	return
}

func (barrier *Barrier) doRemote(ctx context.Context, key []byte, fn func() (result interface{}, err error)) (r interface{}, err error) {
	sk := bytex.ToString(key)
	lockErr := barrier.values.LockWithLease(ctx, sk, barrier.ttl)
	if lockErr != nil {
		err = errors.Warning("hazelcast: barrier failed").WithCause(lockErr)
		return
	}

	value, getErr := barrier.values.Get(ctx, sk)
	if getErr != nil {
		_ = barrier.values.Unlock(ctx, sk)
		err = errors.Warning("hazelcast: barrier failed").WithCause(getErr)
		return
	}
	var content []byte
	has := value != nil
	if has {
		content, has = value.([]byte)
	}
	if !has {
		bv := NewBarrierValue()
		setErr := barrier.values.SetWithTTL(ctx, sk, bv.Bytes(), barrier.ttl)
		if setErr != nil {
			_ = barrier.values.Unlock(ctx, sk)
			err = errors.Warning("hazelcast: barrier failed").WithCause(setErr)
			return
		}
	}
	unlockErr := barrier.values.Unlock(ctx, sk)
	if unlockErr != nil {
		err = errors.Warning("hazelcast: barrier failed").WithCause(unlockErr)
		return
	}

	if has {
		bv := BarrierValue(content)
		exist := false
		for i := 0; i < barrier.loops; i++ {
			if exist = bv.Exist(); exist {
				if bv.Forgot() {
					exist = false
					break
				}
				r, err = bv.Value()
				break
			}
			time.Sleep(barrier.interval)
		}
		if !exist {
			r, err = barrier.doRemote(ctx, key, fn)
		}
	} else {
		bv := NewBarrierValue()
		r, err = fn()
		if err != nil {
			bv = bv.Failed(err)
		} else {
			bv, err = bv.Succeed(r)
			if err != nil {
				err = errors.Warning("hazelcast: barrier failed").WithCause(err)
				return
			}
		}
		setErr := barrier.values.SetWithTTL(ctx, sk, bv.Bytes(), barrier.ttl)
		if setErr != nil {
			err = errors.Warning("hazelcast: barrier failed").WithCause(setErr)
			return
		}
	}

	return
}

func (barrier *Barrier) Forget(ctx context.Context, key []byte) {
	if len(key) == 0 {
		key = []byte{'-'}
	}
	barrier.group.Forget(bytex.ToString(key))
	sk := bytex.ToString(key)
	//_, _ = barrier.values.Remove(ctx, sk)
	_ = barrier.values.SetTTL(ctx, sk, 1*time.Second)
	return
}

func NewBarrierValue() BarrierValue {
	p := make([]byte, 0, 1)
	return append(p, 'X')
}

type BarrierValue []byte

func (bv BarrierValue) Exist() bool {
	return len(bv) > 1
}

func (bv BarrierValue) Forgot() bool {
	return len(bv) > 1 && bv[0] == 'G' && bv[1] == 'G'
}

func (bv BarrierValue) Forget() BarrierValue {
	n := bv[:1]
	n[0] = 'G'
	return append(n, 'G')
}

func (bv BarrierValue) Value() (data []byte, err error) {
	if len(bv) < 2 {
		return
	}
	succeed := bv[0] == 'T'
	if succeed {
		if bv[1] == 'N' {
			return
		}
		data = bv[2:]
		return
	}
	if bv[1] == 'C' {
		err = errors.Decode(bv[2:])
	} else if bv[1] == 'S' {
		err = fmt.Errorf(bytex.ToString(bv[2:]))
	}
	return
}

func (bv BarrierValue) Bytes() []byte {
	return bv
}

func (bv BarrierValue) Succeed(v interface{}) (n BarrierValue, err error) {
	if v == nil {
		n = bv[:1]
		n[0] = 'T'
		n = append(n, 'N')
		return
	}
	p, encodeErr := json.Marshal(v)
	if encodeErr != nil {
		err = errors.Warning("hazelcast: set succeed value into barrier value failed").WithCause(encodeErr)
		return
	}
	n = bv[:1]
	n[0] = 'T'
	n = append(n, 'V')
	n = append(n, p...)
	return
}

func (bv BarrierValue) Failed(v error) (n BarrierValue) {
	n = bv[:1]
	n[0] = 'F'
	codeErr, ok := errors.As(v)
	if ok {
		n = append(n, 'C')
		p, _ := codeErr.MarshalJSON()
		n = append(n, p...)
	} else {
		n = append(n, 'S')
		n = append(n, v.Error()...)
	}
	return
}
