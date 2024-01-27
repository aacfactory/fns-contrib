package transactions

import (
	"github.com/aacfactory/errors"
	"github.com/aacfactory/fns-contrib/databases/sql/databases"
	"sync"
	"sync/atomic"
	"time"
	"unsafe"
)

func NewTransaction(id []byte, processId []byte, tx databases.Transaction, deadline time.Time) *Transaction {
	return &Transaction{
		Transaction: tx,
		Id:          unsafe.String(unsafe.SliceData(id), len(id)),
		processId:   processId,
		Acquires:    1,
		Deadline:    deadline,
		closed:      false,
		locker:      new(sync.Mutex),
	}
}

type Transaction struct {
	databases.Transaction
	Id        string
	processId []byte
	Acquires  int64
	Deadline  time.Time
	closed    bool
	locker    sync.Locker
}

func (tx *Transaction) ProcessId() (id []byte) {
	id = tx.processId
	return
}

func (tx *Transaction) Acquire() (err error) {
	tx.locker.Lock()
	if tx.closed {
		err = errors.Warning("sql: transaction has been committed or rollback")
		tx.locker.Unlock()
		return
	}
	atomic.AddInt64(&tx.Acquires, 1)
	tx.locker.Unlock()
	return
}

func (tx *Transaction) Commit() error {
	tx.locker.Lock()
	if tx.closed {
		tx.locker.Unlock()
		return errors.Warning("sql: transaction has been committed or rollback")
	}
	acquires := atomic.AddInt64(&tx.Acquires, -1)
	if acquires < 1 {
		tx.closed = true
		err := tx.Transaction.Commit()
		if err != nil {
			_ = tx.Transaction.Rollback()
		}
		tx.locker.Unlock()
		return err
	}
	tx.locker.Unlock()
	return nil
}

func (tx *Transaction) Rollback() error {
	tx.locker.Lock()
	if tx.closed {
		tx.locker.Unlock()
		return nil
	}
	tx.closed = true
	err := tx.Transaction.Rollback()
	tx.locker.Unlock()
	return err
}

func (tx *Transaction) Closed() (ok bool) {
	tx.locker.Lock()
	ok = tx.closed
	tx.locker.Unlock()
	return
}
