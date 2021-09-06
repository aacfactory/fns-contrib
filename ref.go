package fns_contrib

import (
	"github.com/aacfactory/fns-contrib/authorizations/jwt"
	"github.com/aacfactory/fns-contrib/databases/sql"
)

func init() {
	_ = jwt.Config{}
	_ = sql.Config{}
}
