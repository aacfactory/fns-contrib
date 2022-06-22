package nats

import "github.com/aacfactory/json"

type ClientTLSConfig struct {
	CA   string `json:"ca"`
	Cert string `json:"cert"`
	Key  string `json:"key"`
}

type OptionsConfig struct {
	Name              string           `json:"name"`
	User              string           `json:"user"`
	Password          string           `json:"password"`
	Token             string           `json:"token"`
	EnableCompression bool             `json:"enableCompression"`
	TimeoutSeconds    int              `json:"timeoutSeconds"`
	ClientTLS         *ClientTLSConfig `json:"clientTLS"`
}

type Config struct {
	URI       string                     `json:"uri"`
	Options   *OptionsConfig             `json:"options"`
	Producers map[string]*ProducerConfig `json:"producers"`
	Consumers map[string]*ConsumerConfig `json:"consumers"`
}

type ConsumerConfig struct {
	Handler        string          `json:"handler"`
	HandlerOptions json.RawMessage `json:"handlerOptions"`
	Subject        string          `json:"subject"`
	Queue          string          `json:"queue"`
}

type ProducerConfig struct {
	Size int `json:"size"`
}
