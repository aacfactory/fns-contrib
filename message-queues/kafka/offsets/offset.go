package offsets

import (
	"github.com/aacfactory/configures"
	"github.com/aacfactory/errors"
	"github.com/aacfactory/fns-contrib/message-queues/kafka/configs"
	"github.com/aacfactory/fns/context"
	"github.com/aacfactory/logs"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"
)

type Offset struct {
	Topic       string
	Partition   int32
	At          int64
	LeaderEpoch int32
	Metadata    string
}

type Offsets []Offset

func (offsets Offsets) convert() (v kadm.Offsets) {
	v = make(kadm.Offsets)
	for _, offset := range offsets {
		v.AddOffset(offset.Topic, offset.Partition, offset.At, offset.LeaderEpoch)
	}
	return
}

func (offsets Offsets) kOffsets() (v map[string]map[int32]kgo.Offset) {
	v = make(map[string]map[int32]kgo.Offset)
	for _, offset := range offsets {
		partition, has := v[offset.Topic]
		if !has {
			partition = make(map[int32]kgo.Offset)
		}
		partition[offset.Partition] = kgo.NewOffset().
			At(offset.At).
			WithEpoch(offset.LeaderEpoch)
		v[offset.Topic] = partition
	}
	return
}

type OffsetManagerOptions struct {
	Log    logs.Logger
	Config configures.Config
}

type OffsetManager interface {
	Name() string
	Construct(options OffsetManagerOptions) (err error)
	Get(ctx context.Context, group string, topic string) (offsets Offsets, err error)
	Set(ctx context.Context, group string, offsets Offsets) (err error)
	Shutdown(ctx context.Context)
}

type DefaultOffsetManagerConfig struct {
	configs.Generic
}

type DefaultOffsetManager struct {
	log   logs.Logger
	admin *kadm.Client
}

func (manager *DefaultOffsetManager) Name() string {
	return "default"
}

func (manager *DefaultOffsetManager) Construct(options OffsetManagerOptions) (err error) {
	manager.log = options.Log
	config := DefaultOffsetManagerConfig{}
	configErr := options.Config.As(&config)
	if configErr != nil {
		err = errors.Warning("kafka: offset manager construct failed").WithMeta("offset", manager.Name()).WithCause(configErr)
		return
	}
	client, clientErr := config.NewClient()
	if clientErr != nil {
		err = errors.Warning("kafka: offset manager construct failed").WithMeta("offset", manager.Name()).WithCause(clientErr)
		return
	}
	manager.admin = kadm.NewClient(client)
	return
}

func (manager *DefaultOffsetManager) Get(ctx context.Context, group string, topic string) (offsets Offsets, err error) {
	resp, fetchErr := manager.admin.FetchOffsetsForTopics(ctx, group, topic)
	if fetchErr != nil {
		err = errors.Warning("kafka: offset manager get failed").WithMeta("offset", manager.Name()).WithCause(fetchErr)
		return
	}
	if err = resp.Error(); err != nil {
		err = errors.Warning("kafka: offset manager get failed").WithMeta("offset", manager.Name()).WithCause(fetchErr)
		return
	}
	v := resp.Offsets()
	vLen := len(v)
	if vLen == 0 {
		return
	}

	ps, has := resp.Offsets()[topic]
	if !has {
		return
	}
	psLen := len(ps)
	if psLen == 0 {
		return
	}
	offsets = make(Offsets, 0, psLen)
	for _, off := range ps {
		offsets = append(offsets, Offset{
			Topic:       off.Topic,
			Partition:   off.Partition,
			At:          off.At,
			LeaderEpoch: off.LeaderEpoch,
			Metadata:    off.Metadata,
		})
	}
	return
}

func (manager *DefaultOffsetManager) Set(ctx context.Context, group string, offsets Offsets) (err error) {
	if len(offsets) == 0 {
		return
	}
	cmtErr := manager.admin.CommitAllOffsets(ctx, group, offsets.convert())
	if cmtErr != nil {
		err = errors.Warning("kafka: offset manager set failed").WithMeta("offset", manager.Name()).WithCause(cmtErr)
		return
	}
	return
}

func (manager *DefaultOffsetManager) Shutdown(_ context.Context) {
	manager.admin.Close()
	return
}
