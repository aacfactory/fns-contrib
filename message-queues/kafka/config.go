package kafka

import "github.com/aacfactory/json"

type ClientTLSConfig struct {
	CA   string `json:"ca"`
	Cert string `json:"cert"`
	Key  string `json:"key"`
}

type OptionsConfig struct {
	SASLType       string           `json:"saslType"`
	Algo           string           `json:"algo"`
	Username       string           `json:"username"`
	Password       string           `json:"password"`
	ClientTLS      *ClientTLSConfig `json:"clientTLS"`
	DualStack      bool             `json:"dualStack"`
	TimeoutSeconds int              `json:"timeoutSeconds"`
	ClientId       string           `json:"clientId"`
}

type Config struct {
	Brokers   []string                   `json:"brokers"`
	Options   *OptionsConfig             `json:"options"`
	Producers map[string]*ProducerConfig `json:"producers"`
	Consumers map[string]*ConsumerConfig `json:"consumers"`
}

type ConsumerConfig struct {
	Handler        string          `json:"handler"`
	HandlerOptions json.RawMessage `json:"handlerOptions"`
	GroupId        string          `json:"groupId"`
	AutoCommit     bool            `json:"autoCommit"`
}

type ProducerConfig struct {
	Compression string `json:"compression"`
	Balancer    string `json:"balancer"`
	RequiredAck string `json:"requiredAck"`
	BatchSize   int    `json:"batchSize"`
	Async       bool   `json:"async"`
}

// kafka.Snappy
