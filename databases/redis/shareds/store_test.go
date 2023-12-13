package shareds_test

import (
	"fmt"
	"github.com/aacfactory/fns-contrib/databases/redis/shareds"
	"github.com/aacfactory/fns/context"
	"github.com/redis/rueidis"
	"testing"
	"time"
)

func TestNewStoreWithClient(t *testing.T) {
	client, clientErr := rueidis.NewClient(rueidis.ClientOption{InitAddress: []string{"127.0.0.1:16379"}})
	if clientErr != nil {
		t.Error(clientErr)
		return
	}
	defer client.Close()
	store, storeErr := shareds.NewStoreWithClient(client)
	if storeErr != nil {
		t.Error(storeErr)
		return
	}
	defer store.Close()

	ctx := context.TODO()

	// set
	setErr := store.Set(ctx, []byte("some"), []byte("some_value"))
	if setErr != nil {
		t.Error(fmt.Sprintf("%+v", setErr))
		return
	}

	// set
	p, has, getErr := store.Get(ctx, []byte("some"))
	if getErr != nil {
		t.Error(fmt.Sprintf("%+v", getErr))
		return
	}
	if has {
		t.Log(string(p))
	}
	// set ttl
	setErr = store.SetWithTTL(ctx, []byte("some"), []byte("some_value_ttl"), 3*time.Second)
	if setErr != nil {
		t.Error(fmt.Sprintf("%+v", setErr))
		return
	}
	for i := 0; i < 2; i++ {
		p, has, getErr = store.Get(ctx, []byte("some"))
		if getErr != nil {
			t.Error(fmt.Sprintf("%+v", getErr))
			return
		}
		t.Log(has, string(p))
		time.Sleep(3 * time.Second)
	}

	// incr
	for i := 0; i < 3; i++ {
		n, incrErr := store.Incr(ctx, []byte("some_incr"), 2)
		if incrErr != nil {
			t.Error(fmt.Sprintf("%+v", incrErr))
			return
		}
		t.Log(n)
	}
	// remove
	rmErr := store.Remove(ctx, []byte("some"))
	if rmErr != nil {
		t.Error(fmt.Sprintf("%+v", rmErr))
		return
	}

}
