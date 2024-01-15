package barriers

import (
	cctx "context"
	"fmt"
	"github.com/aacfactory/avro"
	"github.com/aacfactory/configures"
	"github.com/aacfactory/errors"
	"github.com/aacfactory/fns-contrib/databases/redis/configs"
	"github.com/aacfactory/fns/barriers"
	"github.com/aacfactory/fns/commons/avros"
	"github.com/aacfactory/fns/commons/bytex"
	"github.com/aacfactory/fns/context"
	"github.com/redis/rueidis"
	"github.com/redis/rueidis/rueidisaside"
	"golang.org/x/sync/singleflight"
	"time"
)

func succeed(v any) (r Result, err error) {
	if v == nil {
		r = []byte{'S', 'N'}
		return
	}
	p, encodeErr := avro.Marshal(v)
	if encodeErr != nil {
		err = errors.Warning("barrier: new succeed result failed").WithCause(encodeErr)
		return
	}
	r = []byte{'S', 'S'}
	r = append(r, p...)
	return
}

func failed(err error) (r Result) {
	r = []byte{'F'}
	ce, ok := errors.As(err)
	if ok {
		r = append(r, 'C')
		p, _ := avro.Marshal(ce)
		r = append(r, p...)
	} else {
		r = append(r, 'S')
		r = append(r, bytex.FromString(err.Error())...)
	}
	return
}

type Result []byte

func (r Result) Value() (p []byte, err error) {
	if r[0] == 'F' {
		if r[1] == 'S' {
			err = fmt.Errorf(bytex.ToString(r[2:]))
		} else {
			ce := errors.CodeErrorImpl{}
			_ = avro.Unmarshal(r[2:], &ce)
			err = ce
		}
		return
	}
	p = r[2:]
	return
}

func New(config configs.Config, ttl time.Duration, options ...configs.Option) (v barriers.Barrier, err error) {
	opt := configs.Options{}
	for _, option := range options {
		option(&opt)
	}
	clientOption, clientOptionErr := config.AsOption(opt)
	if clientOptionErr != nil {
		err = errors.Warning("barrier: new failed").WithCause(clientOptionErr)
		return
	}
	ac, acErr := rueidisaside.NewClient(rueidisaside.ClientOption{
		ClientBuilder: nil,
		ClientOption:  clientOption,
		ClientTTL:     0,
	})
	if acErr != nil {
		err = errors.Warning("barrier: new failed").WithCause(acErr)
		return
	}
	if ttl < 1 {
		ttl = 500 * time.Millisecond
	}
	v = &Barrier{
		group:  singleflight.Group{},
		client: ac,
		ttl:    ttl,
		prefix: []byte("fns:barrier:"),
	}
	return
}

func NewWithClient(client rueidis.Client, ttl time.Duration) (v barriers.Barrier, err error) {
	opt := rueidisaside.ClientOption{
		ClientBuilder: func(option rueidis.ClientOption) (rueidis.Client, error) {
			return client, nil
		},
		ClientOption: rueidis.ClientOption{},
		ClientTTL:    0,
	}
	ac, acErr := rueidisaside.NewClient(opt)
	if acErr != nil {
		err = errors.Warning("barrier: new failed").WithCause(acErr)
		return
	}
	if ttl < 1 {
		ttl = 5 * time.Second
	}
	v = &Barrier{
		group:  singleflight.Group{},
		client: ac,
		ttl:    ttl,
		prefix: []byte("fns:barrier:"),
	}
	return
}

func BarrierBuilder(options ...configs.Option) barriers.BarrierBuilder {
	opt := configs.Options{}
	for _, option := range options {
		option(&opt)
	}
	return &barrierBuilder{
		options: opt,
	}
}

type barrierBuilder struct {
	options configs.Options
}

func (builder *barrierBuilder) Build(ctx context.Context, config configures.Config) (barrier barriers.Barrier, err error) {
	conf := configs.Config{}
	configErr := config.As(&conf)
	if configErr != nil {
		err = errors.Warning("redis: new barrier failed").WithCause(configErr)
		return
	}

	clientOption, clientOptionErr := conf.AsOption(builder.options)
	if clientOptionErr != nil {
		err = errors.Warning("redis: new barrier failed").WithCause(clientOptionErr)
		return
	}
	ac, acErr := rueidisaside.NewClient(rueidisaside.ClientOption{
		ClientBuilder: nil,
		ClientOption:  clientOption,
		ClientTTL:     0,
	})
	if acErr != nil {
		err = errors.Warning("redis: new barrier failed").WithCause(acErr)
		return
	}

	barrier = &Barrier{
		group:  singleflight.Group{},
		client: ac,
		ttl:    1 * time.Second,
		prefix: []byte("fns:barrier:"),
	}
	return
}

type Barrier struct {
	group  singleflight.Group
	client rueidisaside.CacheAsideClient
	ttl    time.Duration
	prefix []byte
}

func (b *Barrier) Do(ctx context.Context, key []byte, fn func() (result any, err error)) (result barriers.Result, err error) {
	key = append(b.prefix, key...)
	sk := bytex.ToString(key)
	v, doErr, _ := b.group.Do(sk, func() (v interface{}, err error) {
		val, getErr := b.client.Get(ctx, b.ttl, sk, func(ctx cctx.Context, key string) (val string, err error) {
			fv, fErr := fn()
			if fErr != nil {
				val = bytex.ToString(failed(fErr))
				return
			}
			r, rErr := succeed(fv)
			if rErr != nil {
				err = rErr
				return
			}
			val = bytex.ToString(r)
			return
		})
		if getErr != nil {
			err = getErr
			return
		}
		r := Result(bytex.FromString(val))
		p, rErr := r.Value()
		if rErr != nil {
			err = rErr
			return
		}
		v = p
		return
	})
	b.group.Forget(sk)
	if doErr != nil {
		err = doErr
		return
	}
	result = avros.RawMessage(v.([]byte))
	return
}

func (b *Barrier) Forget(_ context.Context, _ []byte) {
	return
}
