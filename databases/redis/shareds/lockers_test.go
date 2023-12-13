package shareds_test

import (
	"github.com/aacfactory/fns-contrib/databases/redis/shareds"
	"github.com/aacfactory/fns/context"
	rs "github.com/aacfactory/fns/shareds"
	"github.com/redis/rueidis"

	"sync"
	"testing"
)

func TestLockers_Acquire(t *testing.T) {
	client, clientErr := rueidis.NewClient(rueidis.ClientOption{InitAddress: []string{"127.0.0.1:16379"}})
	if clientErr != nil {
		t.Error(clientErr)
		return
	}
	defer client.Close()
	lockers, lockersErr := shareds.NewLockersWithClient(client)
	if lockersErr != nil {
		t.Error(lockersErr)
		return
	}
	x := 0
	wg := new(sync.WaitGroup)
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func(lockers rs.Lockers, wg *sync.WaitGroup, n int) {
			ctx := context.TODO()
			locker, lockerErr := lockers.Acquire(ctx, []byte("some_locker"), 0)
			if lockerErr != nil {
				t.Error(lockerErr)
				return
			}
			lockErr := locker.Lock(ctx)
			if lockErr != nil {
				t.Error(lockErr)
				return
			}
			x++
			t.Log(n, x)
			unlockErr := locker.Unlock(ctx)
			if unlockErr != nil {
				t.Error(unlockErr)
				return
			}
			wg.Done()
		}(lockers, wg, i)
	}
	wg.Wait()
	t.Log(x == 10)
}
