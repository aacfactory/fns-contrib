package internal

import (
	"context"
	"github.com/aacfactory/errors"
	"github.com/aacfactory/logs"
)

type Database struct {
	log    logs.Logger
	client Client
}

func (db *Database) HandleCommand(ctx context.Context, name string, params []interface{}) (result []byte, err errors.CodeError) {

	return
}

func (db *Database) Close() {
	err := db.client.Close()
	if db.log.DebugEnabled() {
		if err == nil {
			db.log.Debug().Caller().Message("redis: close")
		} else {
			db.log.Debug().Caller().Cause(err).Message("redis: close failed")
		}
	}
}
