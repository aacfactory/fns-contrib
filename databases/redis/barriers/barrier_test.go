package barriers_test

import (
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
		result = []string{time.Now().Format(time.RFC3339)}
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
