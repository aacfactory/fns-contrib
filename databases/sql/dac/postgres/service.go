package postgres

import (
	"github.com/aacfactory/fns-contrib/databases/sql"
	"github.com/aacfactory/fns-contrib/databases/sql/dac/postgres/dialect"
	"github.com/aacfactory/fns-contrib/databases/sql/databases"
	"github.com/aacfactory/fns/services"
)

func WithName(name string) sql.Option {
	return sql.WithName(name)
}

func WithDatabase(db databases.Database) sql.Option {
	return sql.WithDatabase(db)
}

func New(options ...sql.Option) services.Service {
	options = append(options, sql.WithDialect(dialect.Name))
	return sql.New(options...)
}
