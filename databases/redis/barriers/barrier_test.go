package barriers_test

import (
	"github.com/aacfactory/errors"
	"github.com/aacfactory/fns-contrib/databases/redis/barriers"
	"github.com/aacfactory/fns/commons/objects"
	"github.com/aacfactory/fns/context"
	"github.com/redis/rueidis"
	"testing"
	"time"
)

func TestBarrier_Do(t *testing.T) {
	client, clientErr := rueidis.NewClient(rueidis.ClientOption{InitAddress: []string{"127.0.0.1:16379"}})
	if clientErr != nil {
		t.Error(clientErr)
		return
	}
	barrier, barrierErr := barriers.NewWithClient(client, 3*time.Second)
	if barrierErr != nil {
		t.Error(barrierErr)
		return
	}
	key := []byte("some")
	r, doErr := barrier.Do(context.TODO(), key, func() (result interface{}, err error) {
		t.Log("do", time.Now())
		result = []time.Time{time.Now()}
		return
	})
	if doErr != nil {
		t.Error(doErr)
		return
	}
	now, nowErr := objects.Value[[]time.Time](r)
	if nowErr != nil {
		t.Error(nowErr)
		return
	}
	t.Log(now)
}

func TestBarrier_DoFailed(t *testing.T) {
	client, clientErr := rueidis.NewClient(rueidis.ClientOption{InitAddress: []string{"127.0.0.1:16379"}})
	if clientErr != nil {
		t.Error(clientErr)
		return
	}
	barrier, barrierErr := barriers.NewWithClient(client, 3*time.Second)
	if barrierErr != nil {
		t.Error(barrierErr)
		return
	}
	key := []byte("some")
	r, doErr := barrier.Do(context.TODO(), key, func() (result interface{}, err error) {
		t.Log("do", time.Now())
		err = errors.Warning("redis: barrier failed")
		//err = fmt.Errorf("redis: cc barrier failed")
		return
	})
	if doErr != nil {
		t.Error(doErr)
		return
	}
	now, nowErr := objects.Value[[]time.Time](r)
	if nowErr != nil {
		t.Error(nowErr)
		return
	}
	t.Log(now)

}
