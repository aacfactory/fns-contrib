package configs

import (
	"fmt"
	"github.com/aacfactory/configures"
	"github.com/aacfactory/errors"
	"github.com/aacfactory/json"
	"github.com/twmb/franz-go/pkg/kgo"
	"hash/fnv"
)

type Partitioner struct {
	Name    string          `json:"name"`
	Options json.RawMessage `json:"options"`
}

func (partitioner Partitioner) Config() (v kgo.Partitioner, err error) {
	if partitioner.Name == "" {
		return
	}
	if len(partitioner.Options) == 0 {
		partitioner.Options = json.EmptyObjectBytes
	}
	config, configErr := configures.NewJsonConfig(partitioner.Options)
	if configErr != nil {
		err = errors.Warning("kafka: config partitioner failed").WithCause(configErr)
		return
	}
	switch partitioner.Name {
	case "round_robin":
		v = kgo.RoundRobinPartitioner()
		break
	case "least_backup":
		v = kgo.LeastBackupPartitioner()
		break
	case "uniform_bytes":
		var opt = struct {
			Bytes    int    `json:"bytes"`
			Adaptive bool   `json:"adaptive"`
			Keys     bool   `json:"keys"`
			Hash     string `json:"hash"`
		}{}
		err = config.As(&opt)
		if err != nil {
			err = errors.Warning("kafka: config partitioner failed").WithCause(err)
			return
		}
		var hasher kgo.PartitionerHasher
		if opt.Hash == "sarama" {
			hasher = kgo.SaramaHasher(func(p []byte) uint32 {
				h := fnv.New32a()
				_, _ = h.Write(p)
				return h.Sum32()
			})
		}
		v = kgo.UniformBytesPartitioner(opt.Bytes, opt.Adaptive, opt.Keys, hasher)
		break
	case "sticky":

		v = kgo.StickyPartitioner()
		break
	case "sticky_key":
		var opt = struct {
			Hash string `json:"hash"`
		}{}
		err = config.As(&opt)
		if err != nil {
			err = errors.Warning("kafka: config partitioner failed").WithCause(err)
			return
		}
		var hasher kgo.PartitionerHasher
		if opt.Hash == "sarama" {
			hasher = kgo.SaramaHasher(func(p []byte) uint32 {
				h := fnv.New32a()
				_, _ = h.Write(p)
				return h.Sum32()
			})
		}
		v = kgo.StickyKeyPartitioner(hasher)
	default:
		err = errors.Warning("kafka: config partitioner failed").WithCause(fmt.Errorf("%s is not supported", partitioner.Name))
		return
	}
	return
}
