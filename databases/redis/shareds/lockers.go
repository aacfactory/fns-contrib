package shareds

import (
	"fmt"
	"github.com/aacfactory/errors"
	"github.com/aacfactory/fns-contrib/databases/redis/configs"
	"github.com/aacfactory/fns/commons/bytex"
	"github.com/aacfactory/fns/context"
	"github.com/aacfactory/fns/shareds"
	"github.com/redis/rueidis"
	"github.com/redis/rueidis/rueidislock"
	"time"
)

type Locker struct {
	raw    rueidislock.Locker
	key    []byte
	cancel context.CancelFunc
}

func (locker *Locker) Lock(ctx context.Context) (err error) {
	_, cancel, lErr := locker.raw.WithContext(ctx, bytex.ToString(locker.key))
	if lErr != nil {
		err = errors.Warning("redis: shared lock failed").WithCause(lErr)
		return
	}
	locker.cancel = context.CancelFunc(cancel)
	return
}

func (locker *Locker) Unlock(ctx context.Context) (err error) {
	locker.cancel()
	return
}

// NewLockers
// redis version must gt 7.0.5
func NewLockers(config configs.Config, options ...configs.Option) (lockers shareds.Lockers, err error) {
	opt := configs.Options{}
	for _, option := range options {
		option(&opt)
	}
	clientOption, clientOptionErr := config.AsOption(opt)
	if clientOptionErr != nil {
		err = errors.Warning("redis: new shared lockers failed").WithCause(clientOptionErr)
		return
	}
	option := rueidislock.LockerOption{
		ClientBuilder:  nil,
		KeyPrefix:      "fns:shared:lockers_rds:",
		ClientOption:   clientOption,
		KeyValidity:    0,
		ExtendInterval: 0,
		TryNextAfter:   0,
		KeyMajority:    0,
		NoLoopTracking: true,
		FallbackSETPX:  false,
	}
	rl, rlErr := rueidislock.NewLocker(option)
	if rlErr != nil {
		err = errors.Warning("redis: new shared lockers failed").WithCause(rlErr)
		return
	}

	lockers = &Lockers{
		raw:    rl,
		shared: false,
	}

	return
}

// NewLockersWithClient
// redis version must gt 7.0.5
func NewLockersWithClient(client rueidis.Client) (lockers shareds.Lockers, err error) {
	option := rueidislock.LockerOption{
		ClientBuilder: func(option rueidis.ClientOption) (rueidis.Client, error) {
			return client, nil
		},
		KeyPrefix:      "fns:shared:lockers_rds:",
		KeyValidity:    0,
		ExtendInterval: 0,
		TryNextAfter:   0,
		KeyMajority:    0,
		NoLoopTracking: true,
		FallbackSETPX:  false,
	}
	rl, rlErr := rueidislock.NewLocker(option)
	if rlErr != nil {
		err = errors.Warning("redis: new shared lockers failed").WithCause(rlErr)
		return
	}

	lockers = &Lockers{
		raw:    rl,
		shared: true,
	}
	return
}

type Lockers struct {
	raw    rueidislock.Locker
	shared bool
}

func (lockers *Lockers) Acquire(_ context.Context, key []byte, _ time.Duration) (locker shareds.Locker, err error) {
	if len(key) == 0 {
		err = errors.Warning("redis: acquire shared locker failed").WithCause(fmt.Errorf("key is required"))
		return
	}
	locker = &Locker{
		raw:    lockers.raw,
		key:    key,
		cancel: nil,
	}
	return
}

func (lockers *Lockers) Close() {
	if lockers.shared {
		return
	}
	lockers.raw.Close()
}
