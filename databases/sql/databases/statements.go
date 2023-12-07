package databases

import (
	"context"
	"database/sql"
	"github.com/aacfactory/errors"
	"github.com/aacfactory/logs"
	"github.com/hashicorp/golang-lru/v2/simplelru"
	"golang.org/x/sync/singleflight"
	"sync/atomic"
	"time"
)

type Preparer interface {
	Prepare(query string) (*sql.Stmt, error)
}

var (
	ErrStatementClosed = errors.Warning("sql: statement was closed")
)

type Statement struct {
	log          logs.Logger
	closed       atomic.Bool
	used         atomic.Int64
	evictTimeout time.Duration
	value        *sql.Stmt
}

func (stmt *Statement) QueryContext(ctx context.Context, args ...any) (r *sql.Rows, err error) {
	if stmt.closed.Load() {
		err = ErrStatementClosed
		return
	}
	stmt.used.Add(1)
	r, err = stmt.value.QueryContext(ctx, args...)
	stmt.used.Add(-1)
	return
}

func (stmt *Statement) ExecContext(ctx context.Context, args ...any) (r sql.Result, err error) {
	if stmt.closed.Load() {
		err = ErrStatementClosed
		return
	}
	stmt.used.Add(1)
	r, err = stmt.value.ExecContext(ctx, args...)
	stmt.used.Add(-1)
	return
}

func (stmt *Statement) Stmt() (v *sql.Stmt, release func(), closed bool) {
	closed = stmt.Closed()
	if closed {
		return
	}
	stmt.used.Add(1)
	v = stmt.value
	release = func() {
		stmt.used.Add(-1)
	}
	return
}

func (stmt *Statement) Closed() bool {
	return stmt.closed.Load()
}

func (stmt *Statement) evict() {
	stmt.closed.Store(true)
	ch := make(chan struct{}, 1)
	go func(stmt *Statement, ch chan struct{}) {
		for {
			if stmt.used.Load() == 0 {
				break
			}
		}
		ch <- struct{}{}
		close(ch)
	}(stmt, ch)
	select {
	case <-ch:
		break
	case <-time.After(stmt.evictTimeout):
		if stmt.log.WarnEnabled() {
			stmt.log.Warn().With("sql", "statement").Message("sql: close statement timeout")
		}
		break
	}
	err := stmt.value.Close()
	if err != nil {
		if stmt.log.WarnEnabled() {
			stmt.log.Warn().With("sql", "statement").
				Cause(errors.Warning("sql: close statement failed").WithCause(err).WithMeta("sql", "statement")).
				Message("sql: close statement failed")
		}
	}
	return
}

type StatementsConfig struct {
	Enable              bool `json:"enable"`
	CacheSize           int  `json:"cacheSize"`
	EvictTimeoutSeconds int  `json:"evictTimeoutSeconds"`
}

func NewStatements(log logs.Logger, preparer Preparer, size int, evictTimeout time.Duration) (v *Statements, err error) {
	if size < 1 {
		size = 256
	}
	if evictTimeout < 1 {
		evictTimeout = 10 * time.Second
	}
	pool, poolErr := simplelru.NewLRU[string, *Statement](size, func(key string, value *Statement) {
		value.evict()
	})
	if poolErr != nil {
		err = errors.Warning("sql: new statements failed").WithCause(poolErr)
		return
	}
	v = &Statements{
		log:          log.With("sql", "statements"),
		evictTimeout: evictTimeout,
		preparer:     preparer,
		pool:         pool,
		group:        singleflight.Group{},
	}
	return
}

type Statements struct {
	log          logs.Logger
	evictTimeout time.Duration
	preparer     Preparer
	pool         *simplelru.LRU[string, *Statement]
	group        singleflight.Group
}

func (stmts *Statements) Get(query string) (stmt *Statement, err error) {
	has := false
	stmt, has = stmts.pool.Get(query)
	if has {
		if stmt.closed.Load() {
			stmt, err = stmts.Get(query)
			return
		}
		return
	}
	v, groupErr, _ := stmts.group.Do(query, func() (v interface{}, err error) {
		value, prepareErr := stmts.preparer.Prepare(query)
		if prepareErr != nil {
			err = prepareErr
			return
		}
		st := &Statement{
			log:          stmts.log,
			closed:       atomic.Bool{},
			used:         atomic.Int64{},
			evictTimeout: stmts.evictTimeout,
			value:        value,
		}
		stmts.pool.Add(query, st)
		v = st
		return
	})
	if groupErr != nil {
		err = groupErr
		return
	}
	stmt = v.(*Statement)
	return
}

func (stmts *Statements) Close() {
	stmts.pool.Purge()
}
