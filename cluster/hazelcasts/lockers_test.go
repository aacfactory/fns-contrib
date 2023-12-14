package hazelcasts_test

import (
	"github.com/aacfactory/fns/context"
	"github.com/hazelcast/hazelcast-go-client"
	"testing"
	"time"
)

func TestLocker_Lock(t *testing.T) {
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

	m, mErr := client.GetMap(context.TODO(), "some")
	if mErr != nil {
		t.Error(mErr)
		return
	}

	ctx := context.Wrap(m.NewLockContext(context.TODO()))
	lockErr := m.LockWithLease(ctx, "abc", 3*time.Second)
	if lockErr != nil {
		t.Error(lockErr)
		return
	}
	t.Log("xxx")
	unlockErr := m.Unlock(ctx, "abc")
	if unlockErr != nil {
		t.Error(unlockErr)
		return
	}
}
