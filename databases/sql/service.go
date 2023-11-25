package sql

import (
	"fmt"
	"github.com/aacfactory/configures"
	"github.com/aacfactory/errors"
	"github.com/aacfactory/fns-contrib/databases/sql/databases"
	"github.com/aacfactory/fns-contrib/databases/sql/transactions"
	"github.com/aacfactory/fns/services"
	"strings"
	"time"
)

var (
	endpointName = []byte("sql")
)

func WithName(name string) Option {
	return func(options *Options) {
		if name == "" {
			return
		}
		options.name = name
	}
}

func WithDatabase(db databases.Database) Option {
	return func(options *Options) {
		options.db = db
	}
}

type Options struct {
	name string
	db   databases.Database
}

type Option func(options *Options)

func New(options ...Option) (v services.Service) {
	opt := Options{
		name: string(endpointName),
		db:   nil,
	}
	for _, option := range options {
		option(&opt)
	}
	v = &service{
		Abstract: services.NewAbstract(opt.name, true),
		db:       nil,
		group:    nil,
	}
	return
}

type service struct {
	services.Abstract
	db        databases.Database
	group     *transactions.Group
	isolation databases.Isolation
}

func (svc *service) Construct(options services.Options) (err error) {
	err = svc.Abstract.Construct(options)
	if err != nil {
		return
	}
	config := Config{}
	configErr := options.Config.As(config)
	if configErr != nil {
		err = errors.Warning(fmt.Sprintf("fns: %s construct failed", svc.Name())).WithMeta("service", svc.Name()).WithCause(configErr)
		return
	}
	if config.Options == nil {
		config.Options = []byte{'{', '}'}
	}

	switch strings.ToLower(config.Kind) {
	case "standalone":
		svc.db = databases.Standalone()
		break
	case "masterSlave":
		svc.db = databases.MasterSlave()
		break
	case "cluster":
		svc.db = databases.Cluster()
		break
	default:
		if svc.db == nil {
			err = errors.Warning(fmt.Sprintf("fns: %s construct failed", svc.Name())).WithMeta("service", svc.Name()).WithCause(fmt.Errorf("%s database was not found", config.Kind))
			return
		}
		break
	}
	dbConfig, dbConfigErr := configures.NewJsonConfig(config.Options)
	if dbConfigErr != nil {
		err = errors.Warning(fmt.Sprintf("fns: %s construct failed", svc.Name())).WithMeta("service", svc.Name()).WithCause(dbConfigErr)
		return
	}
	err = svc.db.Construct(databases.Options{
		Log:    svc.Log().With("database", svc.db.Name()),
		Config: dbConfig,
	})
	if err != nil {
		err = errors.Warning(fmt.Sprintf("fns: %s construct failed", svc.Name())).WithMeta("service", svc.Name()).WithCause(err)
		return
	}
	svc.group = transactions.New(svc.Log(), time.Duration(config.TransactionMaxAge)*time.Second)
	isolation := config.Isolation
	if isolation < 0 || isolation > 7 {
		isolation = databases.LevelReadCommitted
	}
	svc.isolation = isolation
	svc.AddFunction(&transactionBeginFn{
		endpointId: svc.Id(),
		isolation:  svc.isolation,
		db:         svc.db,
		group:      svc.group,
	})
	svc.AddFunction(&transactionCommitFn{
		endpointId: svc.Id(),
		db:         svc.db,
		group:      svc.group,
	})
	svc.AddFunction(&transactionRollbackFn{
		endpointId: svc.Id(),
		db:         svc.db,
		group:      svc.group,
	})
	svc.AddFunction(&queryFn{
		db:    svc.db,
		group: svc.group,
	})
	svc.AddFunction(&executeFn{
		db:    svc.db,
		group: svc.group,
	})
	return
}
