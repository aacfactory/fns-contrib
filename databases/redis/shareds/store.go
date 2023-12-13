package shareds

import (
	"github.com/aacfactory/configures"
	"github.com/aacfactory/errors"
	"github.com/aacfactory/fns-contrib/databases/redis/configs"
	"github.com/aacfactory/fns/commons/bytex"
	"github.com/aacfactory/fns/context"
	"github.com/aacfactory/fns/shareds"
	"github.com/redis/rueidis"
	"time"
)

func NewStore(config configs.Config, options ...configs.Option) (store shareds.Store, err error) {
	opt := configs.Options{}
	for _, option := range options {
		option(&opt)
	}
	client, clientErr := config.Make(opt)
	if clientErr != nil {
		err = errors.Warning("redis: new shared store failed").WithCause(clientErr)
		return
	}
	store = &Store{
		client: client,
		prefix: []byte("fns:shared:store_rds:"),
		shared: false,
	}
	return
}

func StoreBuilder(options ...configs.Option) shareds.StoreBuilder {
	opt := configs.Options{}
	for _, option := range options {
		option(&opt)
	}
	return &storeBuilder{
		options: opt,
	}
}

type storeBuilder struct {
	options configs.Options
}

func (builder *storeBuilder) Build(ctx context.Context, config configures.Config) (store shareds.Store, err error) {
	conf := configs.Config{}
	configErr := config.As(&conf)
	if configErr != nil {
		err = errors.Warning("redis: new shared store failed").WithCause(configErr)
		return
	}
	client, clientErr := conf.Make(builder.options)
	if clientErr != nil {
		err = errors.Warning("redis: new shared store failed").WithCause(clientErr)
		return
	}
	store = &Store{
		client: client,
		prefix: []byte("fns:shared:store_rds:"),
		shared: false,
	}
	return
}

func NewStoreWithClient(client rueidis.Client) (store shareds.Store, err error) {
	store = &Store{
		client: client,
		prefix: []byte("fns:shared:store_rds:"),
		shared: true,
	}
	return
}

type Store struct {
	client rueidis.Client
	shared bool
	prefix []byte
	ttl    time.Duration
}

func (store *Store) Get(ctx context.Context, key []byte) (value []byte, has bool, err error) {
	if len(key) == 0 {
		return
	}
	key = append(store.prefix, key...)
	value, err = store.client.DoCache(ctx, store.client.B().Get().Key(bytex.ToString(key)).Cache(), store.ttl).AsBytes()
	if err != nil {
		if rueidis.IsRedisNil(err) {
			err = nil
			return
		}
		err = errors.Warning("shared: get failed").WithMeta("store", "redis").WithCause(err)
		return
	}
	has = true
	return
}

func (store *Store) Set(ctx context.Context, key []byte, value []byte) (err error) {
	if len(key) == 0 {
		return
	}
	key = append(store.prefix, key...)
	err = store.client.Do(ctx, store.client.B().Set().Key(bytex.ToString(key)).Value(bytex.ToString(value)).Build()).Error()
	if err != nil {
		err = errors.Warning("shared: set failed").WithMeta("store", "redis").WithCause(err)
		return
	}
	return
}

func (store *Store) SetWithTTL(ctx context.Context, key []byte, value []byte, ttl time.Duration) (err error) {
	if len(key) == 0 {
		return
	}
	key = append(store.prefix, key...)
	err = store.client.Do(ctx, store.client.B().Set().Key(bytex.ToString(key)).Value(bytex.ToString(value)).Px(ttl).Build()).Error()
	if err != nil {
		err = errors.Warning("shared: set failed").WithMeta("store", "redis").WithCause(err)
		return
	}
	return
}

func (store *Store) Incr(ctx context.Context, key []byte, delta int64) (v int64, err error) {
	if len(key) == 0 || delta == 0 {
		return
	}
	key = append(store.prefix, key...)
	if delta > 0 {
		v, err = store.client.Do(ctx, store.client.B().Incrby().Key(bytex.ToString(key)).Increment(delta).Build()).AsInt64()
	} else {
		v, err = store.client.Do(ctx, store.client.B().Decrby().Key(bytex.ToString(key)).Decrement(delta*-1).Build()).AsInt64()
	}
	if err != nil {
		err = errors.Warning("shared: incr failed").WithMeta("store", "redis").WithCause(err)
		return
	}
	return
}

func (store *Store) Remove(ctx context.Context, key []byte) (err error) {
	if len(key) == 0 {
		return
	}
	key = append(store.prefix, key...)
	err = store.client.Do(ctx, store.client.B().Del().Key(bytex.ToString(key)).Build()).Error()
	if err != nil {
		err = errors.Warning("shared: remove failed").WithMeta("store", "redis").WithCause(err)
		return
	}
	return
}

func (store *Store) Close() {
	if store.shared {
		return
	}
	store.client.Close()
	return
}
