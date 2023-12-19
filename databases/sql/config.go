package sql

import (
	"github.com/aacfactory/afssl/configs"
	"github.com/aacfactory/fns-contrib/databases/sql/databases"
	"github.com/aacfactory/json"
)

type Config struct {
	Kind              string              `json:"kind"`
	Isolation         databases.Isolation `json:"isolation"`
	TransactionMaxAge int                 `json:"transactionMaxAge"`
	DebugLog          bool                `json:"debugLog"`
	SSL               SSLConfig           `json:"ssl"`
	Options           json.RawMessage     `json:"options"`
}

type SSLConfig struct {
	Enable bool `json:"enable"`
	configs.Client
}
