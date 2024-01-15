package databases

import (
	"context"
	"database/sql"
	"github.com/aacfactory/errors"
	"github.com/aacfactory/fns/commons/bytex"
	"github.com/aacfactory/fns/commons/caches/lru"
	"github.com/aacfactory/fns/commons/mmhash"
	"github.com/aacfactory/logs"
	"golang.org/x/sync/singleflight"
	"strconv"
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
	pool := lru.New[uint64, *Statement](size, func(key uint64, value *Statement) {
		value.evict()
	})
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
	pool         *lru.LRU[uint64, *Statement]
	group        singleflight.Group
}

func (stmts *Statements) Get(query []byte) (stmt *Statement, err error) {
	key := mmhash.Sum64(query)
	has := false
	stmt, has = stmts.pool.Get(key)
	if has {
		if stmt.closed.Load() {
			stmt, err = stmts.Get(query)
			return
		}
		return
	}
	groupKey := strconv.FormatUint(key, 16)
	v, groupErr, _ := stmts.group.Do(groupKey, func() (v interface{}, err error) {
		value, prepareErr := stmts.preparer.Prepare(bytex.ToString(query))
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
		stmts.pool.Add(key, st)
		v = st
		return
	})
	stmts.group.Forget(groupKey)
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
