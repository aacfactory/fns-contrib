package postgres

import "github.com/aacfactory/fns-contrib/databases/sql/dal"

func init() {
	dal.RegisterDialectQueryGeneratorBuilder(dialect, &QueryGeneratorBuilder{})
}
