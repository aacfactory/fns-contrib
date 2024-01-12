package hazelcasts_test

import (
	"github.com/aacfactory/avro"
	"github.com/aacfactory/errors"
	"github.com/aacfactory/fns-contrib/cluster/hazelcasts"
	"github.com/aacfactory/fns/barriers"
	"github.com/aacfactory/fns/commons/avros"
	"github.com/aacfactory/fns/commons/objects"
	"github.com/aacfactory/fns/context"
	"github.com/hazelcast/hazelcast-go-client"
	"reflect"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestBarrier_Do(t *testing.T) {
	config := hazelcast.NewConfig()
	config.Cluster.Network.SetAddresses("127.0.0.1:15701")
	config.Cluster.Security.Credentials.Username = ""
	config.Cluster.Security.Credentials.Password = ""
	client, err := hazelcast.StartNewClientWithConfig(context.TODO(), config)
	if err != nil {
		t.Error(err)
		return
	}
	defer client.Shutdown(context.TODO())
	t.Log(client.Name())
	barrier, barrierErr := hazelcasts.NewBarrier(context.TODO(), client, 8)
	if barrierErr != nil {
		t.Error(barrierErr)
		return
	}
	key := []byte("some")
	for i := 0; i < 2; i++ {
		r, doErr := barrier.Do(context.TODO(), key, func() (result interface{}, err error) {
			t.Log("do")
			result = []time.Time{time.Now()}
			return
		})
		if doErr != nil {
			t.Errorf("%+v", doErr)
			return
		}
		now, nowErr := objects.Value[[]time.Time](r)
		if nowErr != nil {
			t.Error(nowErr)
			return
		}
		t.Log(now)
	}
	barrier.Forget(context.TODO(), key)
}

func TestBarrier_DoNilResult(t *testing.T) {
	config := hazelcast.NewConfig()
	config.Cluster.Network.SetAddresses("127.0.0.1:15701")
	config.Cluster.Security.Credentials.Username = ""
	config.Cluster.Security.Credentials.Password = ""
	client, err := hazelcast.StartNewClientWithConfig(context.TODO(), config)
	if err != nil {
		t.Error(err)
		return
	}
	defer client.Shutdown(context.TODO())
	t.Log(client.Name())
	barrier, barrierErr := hazelcasts.NewBarrier(context.TODO(), client, 8)
	if barrierErr != nil {
		t.Error(barrierErr)
		return
	}
	key := []byte("some")
	r, doErr := barrier.Do(context.TODO(), key, func() (result interface{}, err error) {
		t.Log("do")
		return
	})
	if doErr != nil {
		t.Errorf("%+v", doErr)
		return
	}
	t.Log("type r:", reflect.TypeOf(r), len(r.(avros.RawMessage)))
	t.Log(avro.MustMarshal(r))
	barrier.Forget(context.TODO(), key)
}

func TestBarrier_DoFailed(t *testing.T) {
	config := hazelcast.NewConfig()
	config.Cluster.Network.SetAddresses("127.0.0.1:15701")
	config.Cluster.Security.Credentials.Username = ""
	config.Cluster.Security.Credentials.Password = ""
	client, err := hazelcast.StartNewClientWithConfig(context.TODO(), config)
	if err != nil {
		t.Error(err)
		return
	}
	defer client.Shutdown(context.TODO())
	t.Log(client.Name())
	barrier, barrierErr := hazelcasts.NewBarrier(context.TODO(), client, 8)
	if barrierErr != nil {
		t.Error(barrierErr)
		return
	}
	key := []byte("some")
	r, doErr := barrier.Do(context.TODO(), key, func() (result interface{}, err error) {
		t.Log("do", time.Now())
		err = errors.Warning("hazelcast: barrier failed xxx")
		//err = fmt.Errorf("hazelcast: cc barrier failed")
		return
	})
	if doErr != nil {
		t.Errorf("%+v", doErr)
		return
	}
	now, nowErr := objects.Value[[]time.Time](r)
	if nowErr != nil {
		t.Error(nowErr)
		return
	}
	t.Log(now)

}

func TestBarrier_Do2(t *testing.T) {
	config := hazelcast.NewConfig()
	config.Cluster.Network.SetAddresses("127.0.0.1:15701")
	config.Cluster.Security.Credentials.Username = ""
	config.Cluster.Security.Credentials.Password = ""
	client, err := hazelcast.StartNewClientWithConfig(context.TODO(), config)
	if err != nil {
		t.Error(err)
		return
	}
	defer client.Shutdown(context.TODO())
	t.Log(client.Name())

	b1, b1Err := hazelcasts.NewBarrier(context.TODO(), client, 8)
	if b1Err != nil {
		t.Error(b1Err)
		return
	}
	b2, b2Err := hazelcasts.NewBarrier(context.TODO(), client, 8)
	if b2Err != nil {
		t.Error(b2Err)
		return
	}

	wg := new(sync.WaitGroup)
	counter := new(atomic.Int64)

	var fn = func() {
		counter.Add(1)
	}

	key := []byte("sss")
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func(wg *sync.WaitGroup, barrier barriers.Barrier) {
			r, doErr := barrier.Do(context.TODO(), key, func() (result interface{}, err error) {
				fn()
				result = time.Now()
				return
			})
			barrier.Forget(context.TODO(), key)
			wg.Done()
			if doErr != nil {
				t.Errorf("%+v", doErr)
			} else {
				now, nowErr := objects.Value[time.Time](r)
				t.Log(now, nowErr)
			}
		}(wg, b1)
		wg.Add(1)
		go func(wg *sync.WaitGroup, barrier barriers.Barrier) {
			r, doErr := barrier.Do(context.TODO(), key, func() (result interface{}, err error) {
				fn()
				result = time.Now()
				return
			})
			barrier.Forget(context.TODO(), key)
			wg.Done()
			if doErr != nil {
				t.Errorf("%+v", doErr)
			} else {
				now, nowErr := objects.Value[time.Time](r)
				t.Log(now, nowErr)
			}
		}(wg, b2)
	}
	wg.Wait()
	t.Log(counter.Load())

}
