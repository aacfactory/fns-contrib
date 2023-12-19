package configs

import (
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

func (config IsolationLevel) Config() int8 {
	if config == "uncommitted" {
		return 0
	}
	return 1
}

type ConsumerConfig struct {
	MaxWait                  int32               `json:"maxWait"`
	MinBytes                 string              `json:"minBytes"`
	MaxBytes                 string              `json:"maxBytes"`
	MaxPartBytes             string              `json:"maxPartBytes"`
	IsolationLevel           IsolationLevel      `json:"isolationLevel"`
	KeepControl              bool                `json:"keepControl"`
	Rack                     string              `json:"rack"`
	MaxConcurrentFetches     int                 `json:"maxConcurrentFetches"`
	DisableFetchSessions     bool                `json:"disableFetchSessions"`
	KeepRetryableFetchErrors bool                `json:"keepRetryableFetchErrors"`
	OffsetManager            OffsetManagerConfig `json:"offsetManager"`
	Group                    string              `json:"group"`     // group we are in
	Balancers                []GroupBalancer     `json:"balancers"` // balancers we can use
	SessionTimeout           time.Duration       `json:"sessionTimeout"`
	RebalanceTimeout         time.Duration       `json:"rebalanceTimeout"`
	HeartbeatInterval        time.Duration       `json:"heartbeatInterval"`
	RequireStable            bool                `json:"requireStable"`
	BlockRebalanceOnPoll     bool                `json:"blockRebalanceOnPoll"`
	SetAssigned              bool                `json:"setAssigned"`
	SetRevoked               bool                `json:"setRevoked"`
	SetLost                  bool                `json:"setLost"`
	SetCommitCallback        bool                `json:"setCommitCallback"`
	AutocommitDisable        bool                `json:"autocommitDisable"`
	AutocommitGreedy         bool                `json:"autocommitGreedy"`
	AutocommitMarks          bool                `json:"autocommitMarks"`
	AutocommitInterval       time.Duration       `json:"autocommitInterval"`
}
