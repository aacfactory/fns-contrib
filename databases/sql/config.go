package sql

import (
	"github.com/aacfactory/fns-contrib/databases/sql/databases"
	"github.com/aacfactory/json"
)

type Config struct {
	Kind              string              `json:"kind"`
	Isolation         databases.Isolation `json:"isolation"`
	TransactionMaxAge int                 `json:"transactionMaxAge"`
	Options           json.RawMessage     `json:"options"`
}
