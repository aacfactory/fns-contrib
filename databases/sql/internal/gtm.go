package internal

import (
	"context"
	"database/sql"
	"fmt"
	"github.com/aacfactory/fns/service"
	"github.com/aacfactory/logs"
	"sync"
	"time"
)

type GlobalTransaction struct {
	id       string
	tx       *sql.Tx
	times    int
	expireAT time.Time
}

type globalTransactionManagementOptions struct {
	log             logs.Logger
	checkupInterval time.Duration
}

func newGlobalTransactionManagement(options globalTransactionManagementOptions) *globalTransactionManagement {
	checkupInterval := options.checkupInterval
	if checkupInterval < 60*time.Second {
		checkupInterval = 2 * time.Minute
	}
	v := &globalTransactionManagement{
		log:             options.log.With("sql", "gtm"),
		checkupInterval: checkupInterval,
		txMap:           &sync.Map{},
		closeCh:         make(chan struct{}, 1),
		stopCh:          make(chan struct{}, 1),
	}
	v.checkup()
	return v
}

type globalTransactionManagement struct {
	log             logs.Logger
	checkupInterval time.Duration
	txMap           *sync.Map
	closeCh         chan struct{}
	stopCh          chan struct{}
}

func (gtm *globalTransactionManagement) checkup() {
	go func(gtm *globalTransactionManagement) {
		stop := false
		for {
			select {
			case <-gtm.closeCh:
				stop = true
				break
			case <-time.After(gtm.checkupInterval):
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
						if gtm.log.DebugEnabled() {
							gtm.log.Debug().Caller().With("tid", key).Message("gtm: clean db transaction")
						}
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

func (gtm *globalTransactionManagement) Begin(ctx context.Context, db *sql.DB, isolation sql.IsolationLevel) (err error) {
	request, hasRequest := service.GetRequest(ctx)
	if !hasRequest {
		err = fmt.Errorf("gtm: can not get request in context")
		return
	}
	id := request.Id()
	var gt *GlobalTransaction
	value, has := gtm.txMap.Load(id)
	if !has {
		if isolation < 0 || isolation > 7 {
			isolation = 0
		}
		tx, txErr := db.BeginTx(context.TODO(), &sql.TxOptions{
			Isolation: isolation,
			ReadOnly:  false,
		})
		if txErr != nil {
			err = fmt.Errorf("fns globalTransactionManagement: begin failed, %v", txErr)
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
	if gtm.log.DebugEnabled() {
		gtm.log.Debug().With("requestId", id).Message(fmt.Sprintf("gtm: transaction begin %d times", gt.times))
	}
	return
}

func (gtm *globalTransactionManagement) Get(ctx context.Context) (tx *sql.Tx, has bool) {
	request, hasRequest := service.GetRequest(ctx)
	if !hasRequest {
		return
	}
	id := request.Id()
	value, ok := gtm.txMap.Load(id)
	if !ok {
		return
	}
	gt := value.(*GlobalTransaction)
	tx = gt.tx
	has = true
	return
}

func (gtm *globalTransactionManagement) Commit(ctx context.Context) (finished bool, err error) {
	request, hasRequest := service.GetRequest(ctx)
	if !hasRequest {
		err = fmt.Errorf("gtm: can not get request in context")
		return
	}
	id := request.Id()
	value, ok := gtm.txMap.Load(id)
	if !ok {
		err = fmt.Errorf("fns SQL: commit failed for no transaction in context")
		return
	}
	gt := value.(*GlobalTransaction)
	if gtm.log.DebugEnabled() {
		gtm.log.Debug().With("requestId", id).Message(fmt.Sprintf("transaction has begon %d times", gt.times))
	}
	gt.times = gt.times - 1
	if gt.times == 0 {
		if gtm.log.DebugEnabled() {
			gtm.log.Debug().With("requestId", id).Message("begin to commit transaction")
		}
		err = gt.tx.Commit()
		gtm.txMap.Delete(id)
		if gtm.log.DebugEnabled() {
			_, has := gtm.txMap.Load(id)
			if has {
				gtm.log.Debug().With("requestId", id).Message("transaction has be removed failed")
			} else {
				gtm.log.Debug().With("requestId", id).Message("transaction has be removed succeed")
			}
		}
		finished = true
	}
	return
}

func (gtm *globalTransactionManagement) Rollback(ctx context.Context) {
	request, hasRequest := service.GetRequest(ctx)
	if !hasRequest {
		return
	}
	id := request.Id()
	value, ok := gtm.txMap.Load(id)
	if !ok {
		return
	}
	gt := value.(*GlobalTransaction)
	if gtm.log.DebugEnabled() {
		gtm.log.Debug().With("requestId", id).Message("begin to rollback transaction")
	}
	err := gt.tx.Rollback()
	if err != nil {
		if gtm.log.WarnEnabled() {
			gtm.log.Warn().With("requestId", id).Cause(err).Message("rollback transaction failed")
		}
	}
	gtm.txMap.Delete(id)
	if gtm.log.DebugEnabled() {
		_, has := gtm.txMap.Load(id)
		if has {
			gtm.log.Debug().With("requestId", id).Message("transaction has be removed failed")
		} else {
			gtm.log.Debug().With("requestId", id).Message("transaction has be removed succeed")
		}
	}
	return
}

func (gtm *globalTransactionManagement) Close() {
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
