package sql

import (
	"database/sql"
	"github.com/aacfactory/fns"
	"sync"
	"time"
)

type GlobalTransaction struct {
	id        string
	tx        sql.Tx
	doneCh    chan struct{}
	timeout   time.Duration
	timeoutCh chan string
}

func (gt *GlobalTransaction) Checking() {
	go func(gt *GlobalTransaction) {
		select {
		case <-gt.doneCh:
			break
		case <-time.After(gt.timeout):
			_ = gt.tx.Rollback()
			gt.timeoutCh <- gt.id
		}
	}(gt)
}

type GlobalTransactionManagement struct {
	mutex sync.RWMutex
	txMap map[string]*GlobalTransaction
	timeoutCh chan string
}


func (gtm *GlobalTransactionManagement) Set(ctx fns.Context, tx *sql.Tx, timeout time.Duration)  {
	//id := ctx.RequestId()

}

func (gtm *GlobalTransactionManagement) Get(ctx fns.Context) (tx *sql.Tx, has bool) {


	return
}

func (gtm *GlobalTransactionManagement) Del(ctx fns.Context) {


	return
}