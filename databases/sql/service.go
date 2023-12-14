package sql

import (
	stdsql "database/sql"
	"fmt"
	"github.com/aacfactory/configures"
	"github.com/aacfactory/errors"
	"github.com/aacfactory/fns-contrib/databases/sql/databases"
	"github.com/aacfactory/fns-contrib/databases/sql/transactions"
	"github.com/aacfactory/fns/context"
	"github.com/aacfactory/fns/services"
	"strings"
	"time"
)

var (
	endpointName           = []byte("sql")
	endpointNameContextKey = []byte("@fns:sql:endpoint:name")
)

func WithName(name string) Option {
	return func(options *Options) {
		if name == "" {
			return
		}
		options.name = name
	}
}

func WithDialect(dialect string) Option {
	return func(options *Options) {
		if dialect == "" {
			return
		}
		options.dialect = dialect
	}
}

func WithDatabase(db databases.Database) Option {
	return func(options *Options) {
		options.db = db
	}
}

type Options struct {
	name    string
	dialect string
	db      databases.Database
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
		dialect:  opt.dialect,
	}
	return
}

type service struct {
	services.Abstract
	db        databases.Database
	group     *transactions.Group
	isolation databases.Isolation
	dialect   string
	debug     bool
}

func (svc *service) Construct(options services.Options) (err error) {
	err = svc.Abstract.Construct(options)
	if err != nil {
		return
	}
	config := Config{}
	configErr := options.Config.As(&config)
	if configErr != nil {
		err = errors.Warning(fmt.Sprintf("fns: %s construct failed", svc.Name())).WithMeta("service", svc.Name()).WithCause(configErr)
		return
	}
	if config.Options == nil {
		config.Options = []byte{'{', '}'}
	}

	kind := strings.ToLower(config.Kind)
	switch kind {
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
		if svc.db.Name() != kind {
			err = errors.Warning(fmt.Sprintf("fns: %s construct failed", svc.Name())).WithMeta("service", svc.Name()).
				WithCause(fmt.Errorf("%s database was not found", kind))
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
	if svc.dialect == "" {
		drivers := stdsql.Drivers()
		if len(drivers) != 1 {
			err = errors.Warning(fmt.Sprintf("fns: %s construct failed", svc.Name())).WithMeta("service", svc.Name()).WithCause(fmt.Errorf("drivers is not one"))
			return
		}
		switch drivers[0] {
		case "mysql", "mariadb":
			svc.dialect = "mysql"
			break
		case "postgres", "pgx":
			svc.dialect = "postgres"
			break
		case "oracle":
			svc.dialect = "oracle"
			break
		default:
			err = errors.Warning(fmt.Sprintf("fns: %s construct failed", svc.Name())).WithMeta("service", svc.Name()).WithCause(fmt.Errorf("please use WithDialect to set dialect"))
			return
		}
	}
	svc.debug = config.DebugLog
	// fn
	svc.AddFunction(&transactionBeginFn{
		debug:      svc.debug,
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
		debug: svc.debug,
		log:   svc.Log().With("fn", "query"),
		db:    svc.db,
		group: svc.group,
	})
	svc.AddFunction(&executeFn{
		debug: svc.debug,
		log:   svc.Log().With("fn", "execute"),
		db:    svc.db,
		group: svc.group,
	})
	svc.AddFunction(&dialectFn{
		dialect: svc.dialect,
	})
	return
}

func Use(ctx context.Context, endpointName []byte) context.Context {
	ctx.SetLocalValue(endpointNameContextKey, endpointName)
	return ctx
}

func Disuse(ctx context.Context) context.Context {
	ctx.SetLocalValue(endpointNameContextKey, services.EmptyBytes)
	return ctx
}

func used(ctx context.Context) []byte {
	name, _ := context.LocalValue[[]byte](ctx, endpointNameContextKey)
	return name
}

var (
	debugContextKey = []byte("@fns:sql:debug:log")
)

func useDebugLog(ctx context.Context) {
	ctx.SetLocalValue(debugContextKey, true)
}

func debugLogEnabled(ctx context.Context) bool {
	ok, has := context.LocalValue[bool](ctx, debugContextKey)
	if has {
		return ok
	}
	return false
}
