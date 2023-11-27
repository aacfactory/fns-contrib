package dialect

import (
	"github.com/aacfactory/fns-contrib/databases/sql/dac/specifications"
)

func init() {
	specifications.RegisterDialect(&Dialect{})
}
