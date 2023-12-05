package mysql

import (
	"github.com/aacfactory/fns-contrib/databases/mysql/dialect"
	"github.com/aacfactory/fns-contrib/databases/sql/dac/specifications"
)

func init() {
	specifications.RegisterDialect(dialect.NewDialect())
}
