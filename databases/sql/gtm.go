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

type GlobalTransactionManagement struct {
	mutex     sync.RWMutex
	txMap     map[string]*GlobalTransaction
	timeoutCh chan string
	closeCh   chan struct{}
}

func (gtm *GlobalTransactionManagement) Set(ctx fns.Context, tx *sql.Tx, timeout time.Duration) (err error) {
	if timeout < 1*time.Second {
		timeout = 10 * time.Second
	}
	id := ctx.RequestId()
	gtm.mutex.RLock()
	_, has := gtm.txMap[id]
	gtm.mutex.RUnlock()
	if has {
		err = fmt.Errorf("fns GlobalTransactionManagement: set failed, %s can not set again", id)
		return
	}
	gtm.mutex.Lock()
	gt := &GlobalTransaction{
		id:        id,
		tx:        tx,
		doneCh:    make(chan struct{}, 1),
		timeout:   timeout,
		timeoutCh: gtm.timeoutCh,
	}
	gt.checking()
	gtm.txMap[id] = gt
	gtm.mutex.Unlock()
	return
}

func (gtm *GlobalTransactionManagement) Get(ctx fns.Context) (tx *sql.Tx, has bool) {
	id := ctx.RequestId()
	gtm.mutex.RLock()
	gt, hasGt := gtm.txMap[id]
	gtm.mutex.RUnlock()
	if !hasGt {
		return
	}
	tx = gt.tx
	has = true
	return
}

func (gtm *GlobalTransactionManagement) Del(ctx fns.Context) {
	id := ctx.RequestId()
	gtm.mutex.Lock()
	gt, hasGt := gtm.txMap[id]
	if !hasGt {
		gtm.mutex.Unlock()
		return
	}
	close(gt.doneCh)
	delete(gtm.txMap, id)
	gtm.mutex.Unlock()
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
			gtm.mutex.Lock()
			delete(gtm.txMap, id)
			gtm.mutex.Unlock()
		}
	}(gtm)
}

func (gtm *GlobalTransactionManagement) Close() {
	close(gtm.timeoutCh)
	<-gtm.closeCh
	gtm.mutex.Lock()
	defer gtm.mutex.Unlock()
	for _, gt := range gtm.txMap {
		_ = gt.tx.Rollback()
		close(gt.doneCh)
	}
	gtm.txMap = nil
}
