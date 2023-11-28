package postgres

import (
	"github.com/aacfactory/fns-contrib/databases/sql/dac/postgres/dialect"
	"github.com/aacfactory/fns-contrib/databases/sql/dac/specifications"
)

func init() {
	specifications.RegisterDialect(dialect.NewDialect())
}
