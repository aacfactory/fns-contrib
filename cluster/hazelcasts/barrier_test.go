package hazelcasts_test

import (
	"fmt"
	"github.com/aacfactory/fns-contrib/cluster/hazelcasts"
	"github.com/aacfactory/fns/commons/objects"
	"github.com/aacfactory/fns/context"
	"github.com/hazelcast/hazelcast-go-client"
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
	barrier, barrierErr := hazelcasts.NewBarrier(context.TODO(), client)
	if barrierErr != nil {
		t.Error(barrierErr)
		return
	}
	key := []byte("some")
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
	barrier, barrierErr := hazelcasts.NewBarrier(context.TODO(), client)
	if barrierErr != nil {
		t.Error(barrierErr)
		return
	}
	key := []byte("some")
	r, doErr := barrier.Do(context.TODO(), key, func() (result interface{}, err error) {
		t.Log("do", time.Now())
		//err = errors.Warning("hazelcast: barrier failed xxx")
		err = fmt.Errorf("hazelcast: cc barrier failed")
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
