package mysql

import (
	"crypto/tls"
	"github.com/aacfactory/fns-contrib/databases/mysql/dialect"
	"github.com/aacfactory/fns-contrib/databases/sql"
	"github.com/aacfactory/fns-contrib/databases/sql/databases"
	"github.com/aacfactory/fns/context"
	"github.com/aacfactory/fns/services"
)

func WithName(name string) sql.Option {
	return sql.WithName(name)
}

func WithDatabase(db databases.Database) sql.Option {
	return sql.WithDatabase(db)
}

type RegisterTLSFunc func(config *tls.Config) (err error)

func WithTLS(fn RegisterTLSFunc) sql.Option {
	return sql.WithTLS(sql.RegisterTLSFunc(fn))
}

func New(options ...sql.Option) services.Service {
	options = append(options, sql.WithDialect(dialect.Name))
	return sql.New(options...)
}

func Use(ctx context.Context, endpointName []byte) context.Context {
	return sql.Use(ctx, endpointName)
}

func Disuse(ctx context.Context) context.Context {
	return sql.Disuse(ctx)
}
