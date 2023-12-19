package configs

import (
	"github.com/aacfactory/errors"
	"github.com/aacfactory/fns/commons/bytex"
	"github.com/aacfactory/json"
	"github.com/twmb/franz-go/pkg/kgo"
	"time"
)

type OffsetManagerConfig struct {
	Name    string          `json:"name"`
	Options json.RawMessage `json:"options"`
}

type GroupBalancer string

func (config GroupBalancer) Config() kgo.GroupBalancer {
	switch config {
	case "round_robin":
		return kgo.RoundRobinBalancer()
	case "range":
		return kgo.RangeBalancer()
	case "stick":
		return kgo.StickyBalancer()
	default:
		return kgo.CooperativeStickyBalancer()
	}
}

type IsolationLevel string

func (config IsolationLevel) Config() kgo.IsolationLevel {
	if config == "uncommitted" {
		return kgo.ReadUncommitted()
	}
	return kgo.ReadCommitted()
}

type ConsumerConfig struct {
	MaxPollRecords           int             `json:"maxPollRecords"`
	PartitionBuffer          int             `json:"partitionBuffer"`
	MaxWait                  time.Duration   `json:"maxWait"`
	MinBytes                 string          `json:"minBytes"`
	MaxBytes                 string          `json:"maxBytes"`
	MaxPartitionBytes        string          `json:"maxPartitionBytes"`
	Isolation                IsolationLevel  `json:"isolation"`
	KeepControl              bool            `json:"keepControl"`
	Rack                     string          `json:"rack"`
	MaxConcurrentFetches     int             `json:"maxConcurrentFetches"`
	KeepRetryableFetchErrors bool            `json:"keepRetryableFetchErrors"`
	Group                    string          `json:"group"` // group we are in
	Topics                   []string        `json:"topics"`
	Balancers                []GroupBalancer `json:"balancers"` // balancers we can use
	SessionTimeout           time.Duration   `json:"sessionTimeout"`
	RebalanceTimeout         time.Duration   `json:"rebalanceTimeout"`
	HeartbeatInterval        time.Duration   `json:"heartbeatInterval"`
	RequireStable            bool            `json:"requireStable"`
}

func (config *ConsumerConfig) Options() (opts []kgo.Opt, err error) {
	opts = make([]kgo.Opt, 0, 1)
	// maxWait
	if config.MaxWait > 0 {
		opts = append(opts, kgo.FetchMaxWait(config.MaxWait))
	}
	// minBytes
	if minBytes := config.MinBytes; minBytes != "" {
		n, nErr := bytex.ParseBytes(minBytes)
		if nErr != nil {
			err = errors.Warning("kafka: invalid minBytes").WithCause(nErr)
			return
		}
		opts = append(opts, kgo.FetchMinBytes(int32(n)))
	}
	// maxBytes
	if maxBytes := config.MaxBytes; maxBytes != "" {
		n, nErr := bytex.ParseBytes(maxBytes)
		if nErr != nil {
			err = errors.Warning("kafka: invalid maxBytes").WithCause(nErr)
			return
		}
		opts = append(opts, kgo.FetchMaxBytes(int32(n)))
	}
	// maxPartitionBytes
	if maxPartitionBytes := config.MaxPartitionBytes; maxPartitionBytes != "" {
		n, nErr := bytex.ParseBytes(maxPartitionBytes)
		if nErr != nil {
			err = errors.Warning("kafka: invalid maxPartitionBytes").WithCause(nErr)
			return
		}
		opts = append(opts, kgo.FetchMaxPartitionBytes(int32(n)))
	}
	// isolation
	opts = append(opts, kgo.FetchIsolationLevel(config.Isolation.Config()))
	// keepControl
	if config.KeepControl {
		opts = append(opts, kgo.KeepControlRecords())
	}
	// rack
	if config.Rack != "" {
		opts = append(opts, kgo.Rack(config.Rack))
	}
	// maxConcurrentFetches
	if config.MaxConcurrentFetches > 0 {
		opts = append(opts, kgo.MaxConcurrentFetches(config.MaxConcurrentFetches))
	}
	// group
	if config.Group == "" {
		err = errors.Warning("kafka: group is required")
		return
	}
	opts = append(opts, kgo.ConsumerGroup(config.Group))
	// topics
	if len(config.Topics) == 0 {
		err = errors.Warning("kafka: topics are required")
		return
	}
	opts = append(opts, kgo.ConsumeTopics(config.Topics...))
	// balancers
	if balancersLen := len(config.Balancers); balancersLen > 0 {
		balancers := make([]kgo.GroupBalancer, balancersLen)
		for i, balancer := range config.Balancers {
			balancers[i] = balancer.Config()
		}
		opts = append(opts, kgo.Balancers(balancers...))
	}
	// sessionTimeout
	if config.SessionTimeout > 0 {
		opts = append(opts, kgo.SessionTimeout(config.SessionTimeout))
	}
	// heartbeatInterval
	if config.HeartbeatInterval > 0 {
		opts = append(opts, kgo.HeartbeatInterval(config.HeartbeatInterval))
	}
	// requireStable
	if config.RequireStable {
		opts = append(opts, kgo.RequireStableFetchOffsets())
	}
	opts = append(opts, kgo.DisableAutoCommit())
	opts = append(opts, kgo.BlockRebalanceOnPoll())
	return
}
