package sql

import (
	db "database/sql"
	"fmt"
	"github.com/aacfactory/fns"
	"sync"
	"time"
)

type GlobalTransaction struct {
	id        string
	tx        *db.Tx
	times     int
	doneCh    chan struct{}
	timeout   time.Duration
	timeoutCh chan string
}

func (gt *GlobalTransaction) checking() {
	go func(gt *GlobalTransaction) {
		select {
		case <-gt.doneCh:
			break
		case <-time.After(gt.timeout):
			_ = gt.tx.Rollback()
			gt.timeoutCh <- gt.id
			break
		}
	}(gt)
}

func NewGlobalTransactionManagement() *GlobalTransactionManagement {
	return &GlobalTransactionManagement{
		txMap:     sync.Map{},
		timeoutCh: make(chan string, 512),
		closeCh:   make(chan struct{}),
	}
}

type GlobalTransactionManagement struct {
	txMap     sync.Map
	timeoutCh chan string
	closeCh   chan struct{}
}

func (gtm *GlobalTransactionManagement) Begin(ctx fns.Context, db0 *db.DB, isolation db.IsolationLevel, timeout time.Duration) (err error) {
	if timeout < 1*time.Second {
		if ctx.App().ClusterMode() {
			timeout = 3 * time.Second
		} else {
			timeout = 1 * time.Second
		}
	}
	if isolation < 0 || isolation > 7 {
		isolation = 0
	}
	id := ctx.RequestId()
	var gt *GlobalTransaction
	value, has := gtm.txMap.Load(id)
	if !has {
		tx, txErr := db0.BeginTx(ctx, &db.TxOptions{
			Isolation: isolation,
			ReadOnly:  false,
		})
		if txErr != nil {
			err = fmt.Errorf("fns GlobalTransactionManagement: begin failed, %v", txErr)
			return
		}

		gt = &GlobalTransaction{
			id:        id,
			tx:        tx,
			times:     0,
			doneCh:    make(chan struct{}, 1),
			timeout:   timeout,
			timeoutCh: gtm.timeoutCh,
		}
		gt.checking()
	} else {
		gt = value.(*GlobalTransaction)
	}
	gt.times = gt.times + 1
	gtm.txMap.Store(id, gt)
	return
}

func (gtm *GlobalTransactionManagement) GetTx(ctx fns.Context) (tx *db.Tx, has bool) {
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
		err = fmt.Errorf("fns SQL: no tx")
		return
	}
	gt := value.(*GlobalTransaction)
	gt.times = gt.times - 1
	if gt.times < 1 {
		err = gt.tx.Commit()
		_ = gt.tx.Rollback()
		close(gt.doneCh)
		gtm.txMap.Delete(id)
	} else {
		gtm.txMap.Store(id, gt)
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
	_ = gt.tx.Rollback()
	close(gt.doneCh)
	gtm.txMap.Delete(id)
	return
}

func (gtm *GlobalTransactionManagement) handleTimeoutTx() {
	go func(gtm *GlobalTransactionManagement) {
		for {
			id, ok := <-gtm.timeoutCh
			if !ok {
				close(gtm.closeCh)
				break
			}
			gtm.txMap.Delete(id)
		}
	}(gtm)
}

func (gtm *GlobalTransactionManagement) Close() {
	close(gtm.timeoutCh)
	<-gtm.closeCh
	gtm.txMap.Range(func(_, value interface{}) bool {
		gt := value.(*GlobalTransaction)
		_ = gt.tx.Rollback()
		close(gt.doneCh)
		return true
	})
}
