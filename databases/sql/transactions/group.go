package transactions

import (
	"github.com/aacfactory/fns-contrib/databases/sql/databases"
	"github.com/aacfactory/logs"
	"sync"
	"time"
	"unsafe"
)

func New(log logs.Logger, maxAge time.Duration) *Group {
	if maxAge < 1*time.Millisecond {
		maxAge = 10 * time.Second
	}
	group := &Group{
		log:      log.With("transactions", "gtm"),
		maxAge:   maxAge,
		timer:    time.NewTimer(maxAge * 10),
		values:   sync.Map{},
		timeouts: make([]*Transaction, 0, 1),
		closeCh:  make(chan struct{}, 1),
		stopCh:   make(chan struct{}, 1),
	}
	go group.checkup()
	return group
}

type Group struct {
	log      logs.Logger
	maxAge   time.Duration
	timer    *time.Timer
	values   sync.Map
	timeouts []*Transaction
	closeCh  chan struct{}
	stopCh   chan struct{}
}

func (group *Group) Get(id []byte) (tx *Transaction, has bool) {
	value, exist := group.values.Load(unsafe.String(unsafe.SliceData(id), len(id)))
	if exist {
		v, ok := value.(*Transaction)
		if ok {
			tx = v
			has = true
		}
	}
	return
}

func (group *Group) Set(id []byte, tx databases.Transaction) (v *Transaction, ok bool) {
	_, exist := group.values.Load(unsafe.String(unsafe.SliceData(id), len(id)))
	if exist {
		return
	}
	v = NewTransaction(id, tx, time.Now().Add(group.maxAge))
	group.values.Store(unsafe.String(unsafe.SliceData(id), len(id)), v)
	ok = true
	return
}

func (group *Group) Remove(id []byte) {
	key := unsafe.String(unsafe.SliceData(id), len(id))
	value, exist := group.values.Load(key)
	if exist {
		v, ok := value.(*Transaction)
		if ok {
			_ = v.Rollback()
		}
		group.values.Delete(key)
	}
}

func (group *Group) GetAndRemove(id []byte) (tx *Transaction, has bool) {
	value, exist := group.values.LoadAndDelete(unsafe.String(unsafe.SliceData(id), len(id)))
	if exist {
		v, ok := value.(*Transaction)
		if ok {
			tx = v
			has = true
		}
	}
	return
}

func (group *Group) checkup() {
	stop := false
	for {
		select {
		case <-group.closeCh:
			stop = true
			break
		case <-group.timer.C:
			now := time.Now()
			timeouts := group.timeouts[:0]
			group.values.Range(func(_, value interface{}) bool {
				v, ok := value.(*Transaction)
				if ok && v.Deadline.After(now) {
					timeouts = append(timeouts, v)
				}
				return true
			})
			for _, transaction := range timeouts {
				_, has := group.values.LoadAndDelete(transaction.Id)
				if has {
					_ = transaction.Rollback()
					if group.log.DebugEnabled() {
						group.log.Debug().Caller().With("tid", transaction.Id).Message("sql: rollback timeout transaction")
					}
				}
			}
			break
		}
		if stop {
			close(group.stopCh)
			break
		}
		group.timer.Reset(group.maxAge * 10)
	}
	group.timer.Stop()
}

func (group *Group) Close() {
	close(group.closeCh)
	select {
	case <-group.stopCh:
		break
	case <-time.After(10 * time.Second):
		break
	}
	group.values.Range(func(_, value interface{}) bool {
		v, ok := value.(*Transaction)
		if ok {
			_ = v.Rollback()
		}
		return true
	})
}
