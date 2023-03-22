package internal

import (
	"context"
	"github.com/aacfactory/errors"
	"github.com/aacfactory/fns/service"
	"github.com/aacfactory/logs"
	rds "github.com/redis/go-redis/v9"
	"time"
)

type Client interface {
	rds.Cmdable
	Close() (err error)
}

func NewDatabase(name string) (db *Database) {
	return &Database{
		name: name,
	}
}

type Database struct {
	log     logs.Logger
	appId   string
	appName string
	name    string
	client  Client
	gpm     *globalPipelineManagement
}

func (db *Database) Name() (name string) {
	name = db.name
	return
}

func (db *Database) Build(options service.ComponentOptions) (err error) {
	db.log = options.Log
	db.appId = options.AppId
	db.appName = options.AppName
	config := Config{}
	configErr := options.Config.As(&config)
	if configErr != nil {
		err = errors.Warning("redis: build failed").WithCause(configErr)
		return
	}
	client, dailErr := config.Dial(db.appId, db.appName)
	if dailErr != nil {
		err = errors.Warning("redis: build failed").WithCause(dailErr)
		return
	}
	db.client = client
	db.gpm = newGlobalPipelineManagement(globalPipelineManagementOptions{
		log:              db.log,
		maxAliveDuration: 30 * time.Second,
	})
	return
}

func (db *Database) Close() {
	_ = db.client.Close()
}

func (db *Database) Cmder(ctx context.Context) (cmder rds.Cmdable) {
	pipeline, has := db.gpm.Get(ctx)
	if has {
		cmder = pipeline
		return
	}
	cmder = db.client
	return
}

func (db *Database) Pipeline(ctx context.Context) (err error) {
	err = db.gpm.Create(ctx, db.client, false)
	if err != nil {
		err = errors.Warning("redis: create pipeline failed").WithCause(err)
		return
	}
	return
}

func (db *Database) TxPipeline(ctx context.Context) (err error) {
	err = db.gpm.Create(ctx, db.client, true)
	if err != nil {
		err = errors.Warning("redis: create tx pipeline failed").WithCause(err)
		return
	}
	return
}

func (db *Database) Exec(ctx context.Context) (finished bool, cmds []rds.Cmder, err error) {
	finished, cmds, err = db.gpm.Exec(ctx)
	return
}

func (db *Database) Discard(ctx context.Context) {
	db.gpm.Discard(ctx)
	return
}
