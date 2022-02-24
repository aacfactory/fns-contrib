package sql

import (
	db "database/sql"
	"fmt"
	"github.com/aacfactory/fns"
	"sync"
	"time"
)

type GlobalTransaction struct {
	id       string
	tx       *db.Tx
	times    int
	expireAT time.Time
}

func NewGlobalTransactionManagement() *GlobalTransactionManagement {
	v := &GlobalTransactionManagement{
		txMap:   &sync.Map{},
		closeCh: make(chan struct{}, 1),
		stopCh:  make(chan struct{}, 1),
	}
	v.checkup()
	return v
}

type GlobalTransactionManagement struct {
	txMap   *sync.Map
	closeCh chan struct{}
	stopCh  chan struct{}
}

func (gtm *GlobalTransactionManagement) checkup() {
	go func(gtm *GlobalTransactionManagement) {
		stop := false
		for {
			select {
			case <-gtm.closeCh:
				stop = true
				break
			case <-time.After(60 * time.Second):
				now := time.Now()
				timeouts := make(map[string]*GlobalTransaction)
				gtm.txMap.Range(func(_, value interface{}) bool {
					gt := value.(*GlobalTransaction)
					if gt.expireAT.After(now) {
						timeouts[gt.id] = gt
					}
					return true
				})
				for key, transaction := range timeouts {
					_, has := gtm.txMap.LoadAndDelete(key)
					if has {
						_ = transaction.tx.Rollback()
					}
				}
			}
			if stop {
				close(gtm.stopCh)
				break
			}
		}
	}(gtm)
}

func (gtm *GlobalTransactionManagement) Begin(ctx fns.Context, db0 *db.DB, isolation db.IsolationLevel, readOnly bool) (err error) {
	id := ctx.RequestId()
	var gt *GlobalTransaction
	value, has := gtm.txMap.Load(id)
	if !has {
		if isolation < 0 || isolation > 7 {
			isolation = 0
		}
		tx, txErr := db0.BeginTx(ctx, &db.TxOptions{
			Isolation: isolation,
			ReadOnly:  readOnly,
		})
		if txErr != nil {
			err = fmt.Errorf("fns GlobalTransactionManagement: begin failed, %v", txErr)
			return
		}
		gt = &GlobalTransaction{
			id:       id,
			tx:       tx,
			times:    1,
			expireAT: time.Now().Add(10 * time.Second),
		}
		gtm.txMap.Store(id, gt)
	} else {
		gt = value.(*GlobalTransaction)
		gt.times = gt.times + 1
	}
	if ctx.App().Log().DebugEnabled() {
		ctx.App().Log().Debug().With("sql", "gtc").With("requestId", id).Message(fmt.Sprintf("transaction begin %d times", gt.times))
	}
	return
}

func (gtm *GlobalTransactionManagement) Get(ctx fns.Context) (tx *db.Tx, has bool) {
	id := ctx.RequestId()
	value, ok := gtm.txMap.Load(id)
	if !ok {
		return
	}
	gt := value.(*GlobalTransaction)
	tx = gt.tx
	has = true
	return
}

func (gtm *GlobalTransactionManagement) Commit(ctx fns.Context) (err error) {
	id := ctx.RequestId()
	value, ok := gtm.txMap.Load(id)
	if !ok {
		err = fmt.Errorf("fns SQL: commit failed for no transaction in context")
		return
	}
	gt := value.(*GlobalTransaction)
	if ctx.App().Log().DebugEnabled() {
		ctx.App().Log().Debug().With("sql", "gtc").With("requestId", id).Message(fmt.Sprintf("transaction has begon %d times", gt.times))
	}
	gt.times = gt.times - 1
	if gt.times == 0 {
		if ctx.App().Log().DebugEnabled() {
			ctx.App().Log().Debug().With("sql", "gtc").With("requestId", id).Message("begin to commit transaction")
		}
		err = gt.tx.Commit()
		gtm.txMap.Delete(id)
		if ctx.App().Log().DebugEnabled() {
			_, has := gtm.txMap.Load(id)
			if has {
				ctx.App().Log().Debug().With("sql", "gtc").With("requestId", id).Message("transaction has be removed failed")
			} else {
				ctx.App().Log().Debug().With("sql", "gtc").With("requestId", id).Message("transaction has be removed succeed")
			}
		}
	}
	return
}

func (gtm *GlobalTransactionManagement) Rollback(ctx fns.Context) {
	id := ctx.RequestId()
	value, ok := gtm.txMap.Load(id)
	if !ok {
		return
	}
	gt := value.(*GlobalTransaction)
	if ctx.App().Log().DebugEnabled() {
		ctx.App().Log().Debug().With("sql", "gtc").With("requestId", id).Message("begin to rollback transaction")
	}
	err := gt.tx.Rollback()
	if err != nil {
		if ctx.App().Log().WarnEnabled() {
			ctx.App().Log().Warn().With("sql", "gtc").With("requestId", id).Cause(err).Message("rollback transaction failed")
		}
	}
	gtm.txMap.Delete(id)
	if ctx.App().Log().DebugEnabled() {
		_, has := gtm.txMap.Load(id)
		if has {
			ctx.App().Log().Debug().With("sql", "gtc").With("requestId", id).Message("transaction has be removed failed")
		} else {
			ctx.App().Log().Debug().With("sql", "gtc").With("requestId", id).Message("transaction has be removed succeed")
		}
	}
	return
}

func (gtm *GlobalTransactionManagement) Close() {
	close(gtm.closeCh)
	select {
	case <-gtm.stopCh:
		break
	case <-time.After(1 * time.Second):
		break
	}
	gtm.txMap.Range(func(_, value interface{}) bool {
		gt := value.(*GlobalTransaction)
		_ = gt.tx.Rollback()
		return true
	})
}
