package hazelcasts

import (
	"fmt"
	"github.com/aacfactory/avro"
	"github.com/aacfactory/errors"
	"github.com/aacfactory/fns/barriers"
	"github.com/aacfactory/fns/commons/avros"
	"github.com/aacfactory/fns/commons/bytex"
	"github.com/aacfactory/fns/context"
	"github.com/hazelcast/hazelcast-go-client"
	"golang.org/x/sync/singleflight"
	"time"
)

func NewBarrier(ctx context.Context, client *hazelcast.Client, size int) (v barriers.Barrier, err error) {
	mm, mmErr := NewMaps(ctx, "fns:barrier", client, size)
	if mmErr != nil {
		err = errors.Warning("hazelcast: new barrier failed").WithCause(mmErr)
		return
	}
	ttl := 2 * time.Second
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
	values   *Maps
	ttl      time.Duration
	interval time.Duration
	loops    int
	group    singleflight.Group
}

func (barrier *Barrier) Do(ctx context.Context, key []byte, fn func() (result interface{}, err error)) (result barriers.Result, err error) {
	if len(key) == 0 {
		key = []byte{'-'}
	}
	localKey := bytex.ToString(key)
	r, doErr, _ := barrier.group.Do(localKey, func() (r interface{}, err error) {
		r, err = barrier.doRemote(ctx, key, fn)
		return
	})
	barrier.group.Forget(localKey)
	if doErr != nil {
		err = doErr
		return
	}
	result = avros.RawMessage(r.([]byte))
	return
}

func (barrier *Barrier) getValue(ctx context.Context, key []byte) (v BarrierValue, err error) {
	content, has, getErr := barrier.values.Get(ctx, key)
	if getErr != nil {
		err = errors.Warning("hazelcast: barrier get value failed").WithCause(getErr)
		return
	}
	if !has {
		v = NewBarrierValue()
		return
	}
	v = content
	return
}

func (barrier *Barrier) setValue(ctx context.Context, key []byte, value BarrierValue) (err error) {
	err = barrier.values.SetWithTTL(ctx, key, value.Bytes(), barrier.ttl)
	if err != nil {
		err = errors.Warning("hazelcast: barrier set value failed").WithCause(err)
		return
	}
	return
}

func (barrier *Barrier) doRemote(ctx context.Context, key []byte, fn func() (result interface{}, err error)) (r interface{}, err error) {
	ctx = barrier.values.NewLockContext(ctx, key)
	lockErr := barrier.values.LockWithLease(ctx, key, barrier.ttl)
	if lockErr != nil {
		err = errors.Warning("hazelcast: barrier failed").WithCause(lockErr)
		return
	}
	value, getErr := barrier.getValue(ctx, key)
	if getErr != nil {
		_ = barrier.values.Unlock(ctx, key)
		err = errors.Warning("hazelcast: barrier failed").WithCause(getErr)
		return
	}
	if value.IsInit() {
		setErr := barrier.setValue(ctx, key, value.Executing())
		if setErr != nil {
			_ = barrier.values.Unlock(ctx, key)
			err = errors.Warning("hazelcast: barrier failed").WithCause(setErr)
			return
		}
	}
	unlockErr := barrier.values.Unlock(ctx, key)
	if unlockErr != nil {
		err = errors.Warning("hazelcast: barrier failed").WithCause(unlockErr)
		return
	}
	if value.IsInit() {
		r, err = fn()
		if err != nil {
			value = value.Failed(err)
		} else {
			value, err = value.Succeed(r)
			if err != nil {
				err = errors.Warning("hazelcast: barrier failed").WithCause(err)
				return
			}
		}
		setErr := barrier.setValue(ctx, key, value)
		if setErr != nil {
			err = errors.Warning("hazelcast: barrier failed").WithCause(setErr)
			return
		}
		r, err = value.Value()
		return
	}

	if value.Exist() && value.Finished() {
		r, err = value.Value()
	} else {
		exist := false
		for i := 0; i < barrier.loops; i++ {
			value, getErr = barrier.getValue(ctx, key)
			if getErr != nil {
				err = errors.Warning("hazelcast: barrier failed").WithCause(getErr)
				return
			}
			if !value.Exist() {
				break
			}
			if value.IsExecuting() {
				time.Sleep(barrier.interval)
				continue
			}
			exist = true
			break
		}
		if exist {
			r, err = value.Value()
			return
		}
		r, err = barrier.doRemote(ctx, key, fn)
	}
	return
}

func (barrier *Barrier) Forget(_ context.Context, _ []byte) {
	return
}

func NewBarrierValue() BarrierValue {
	return []byte{'X'}
}

type BarrierValue []byte

func (bv BarrierValue) IsInit() bool {
	return len(bv) == 1 && bv[0] == 'X'
}

func (bv BarrierValue) Exist() bool {
	return len(bv) > 1
}

func (bv BarrierValue) Executing() BarrierValue {
	return append(bv, 'X')
}

func (bv BarrierValue) IsExecuting() bool {
	return len(bv) > 1 && bv[0] == 'X' && bv[1] == 'X'
}

func (bv BarrierValue) Finished() bool {
	return len(bv) > 1 && bv[0] != 'X' && bv[1] != 'X'
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
		codeErr := &errors.CodeErrorImpl{}
		err = avro.Unmarshal(bv[2:], codeErr)
		if err != nil {
			err = errors.Warning("hazelcast: decode barrier err result failed").WithCause(err)
			return
		}
		err = codeErr
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
	p, encodeErr := avro.Marshal(v)
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
		p, _ := avro.Marshal(codeErr)
		n = append(n, p...)
	} else {
		n = append(n, 'S')
		n = append(n, v.Error()...)
	}
	return
}
