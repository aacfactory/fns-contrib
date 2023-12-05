package sql

import (
	"github.com/aacfactory/fns-contrib/databases/sql/databases"
	"github.com/aacfactory/json"
)

type Config struct {
	Kind              string              `json:"kind"`
	Dialect           string              `json:"dialect"`
	Isolation         databases.Isolation `json:"isolation"`
	TransactionMaxAge int                 `json:"transactionMaxAge"`
	DebugLog          bool                `json:"debugLog"`
	Options           json.RawMessage     `json:"options"`
}
