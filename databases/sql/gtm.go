package sql

import (
	"database/sql"
	"fmt"
	"github.com/aacfactory/fns"
	"sync"
	"time"
)

type GlobalTransaction struct {
	id        string
	tx        *sql.Tx
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

func (gtm *GlobalTransactionManagement) Set(ctx fns.Context, tx *sql.Tx, timeout time.Duration) (err error) {
	if timeout < 1*time.Second {
		timeout = 10 * time.Second
	}
	id := ctx.RequestId()
	_, has := gtm.Get(ctx)
	if has {
		err = fmt.Errorf("fns GlobalTransactionManagement: set failed, %s can not set again", id)
		return
	}
	gt := &GlobalTransaction{
		id:        id,
		tx:        tx,
		doneCh:    make(chan struct{}, 1),
		timeout:   timeout,
		timeoutCh: gtm.timeoutCh,
	}
	gt.checking()
	gtm.txMap.Store(id, gt)
	return
}

func (gtm *GlobalTransactionManagement) Get(ctx fns.Context) (tx *sql.Tx, has bool) {
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

func (gtm *GlobalTransactionManagement) Del(ctx fns.Context) {
	id := ctx.RequestId()
	value, ok := gtm.txMap.Load(id)
	if !ok {
		return
	}
	gt := value.(*GlobalTransaction)
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
