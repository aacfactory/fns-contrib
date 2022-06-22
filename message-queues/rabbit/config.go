package rabbit

import (
	"github.com/aacfactory/json"
	amqp "github.com/rabbitmq/amqp091-go"
)

type ClientTLSConfig struct {
	CA                 string `json:"ca"`
	Cert               string `json:"cert"`
	Key                string `json:"key"`
	InsecureSkipVerify bool   `json:"insecureSkipVerify"`
}

type OptionsConfig struct {
	AMQPlainAuth     *amqp.AMQPlainAuth `json:"amqPlainAuth"`
	Vhost            string             `json:"vhost"`
	ChannelMax       int                `json:"channelMax"`
	FrameSize        int                `json:"frameSize"`
	HeartbeatSeconds int                `json:"heartbeatSeconds"`
	Locale           string             `json:"locale"`
	ClientTLS        *ClientTLSConfig   `json:"clientTLS"`
}

type Config struct {
	URI       string                     `json:"uri"`
	Options   *OptionsConfig             `json:"options"`
	Producers map[string]*ProducerConfig `json:"producers"`
	Consumers map[string]*ConsumerConfig `json:"consumers"`
}

type ConsumerConfig struct {
	Handler        string                 `json:"handler"`
	HandlerOptions json.RawMessage        `json:"handlerOptions"`
	Queue          string                 `json:"queue"`
	AutoAck        bool                   `json:"autoAck"`
	Exclusive      bool                   `json:"exclusive"`
	NoLocal        bool                   `json:"noLocal"`
	NoWait         bool                   `json:"noWait"`
	Arguments      map[string]interface{} `json:"arguments"`
}

type ProducerConfig struct {
	Exchange    string `json:"exchange"`
	ConfirmMode bool   `json:"confirmMode"`
	Key         string `json:"key"`
	Mandatory   bool   `json:"mandatory"`
	Immediate   bool   `json:"immediate"`
	Size        int    `json:"size"`
}
