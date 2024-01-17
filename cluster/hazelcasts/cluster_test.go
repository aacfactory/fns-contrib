package hazelcasts_test

import (
	"github.com/aacfactory/configures"
	"github.com/aacfactory/fns-contrib/cluster/hazelcasts"
	"github.com/aacfactory/fns-contrib/cluster/hazelcasts/configs"
	fc "github.com/aacfactory/fns/clusters"
	"github.com/aacfactory/fns/commons/versions"
	"github.com/aacfactory/fns/context"
	"github.com/aacfactory/json"
	"github.com/aacfactory/logs"
	"github.com/hazelcast/hazelcast-go-client"
	"reflect"
	"testing"
	"time"
)

func TestClient(t *testing.T) {
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
	mm, getMMErr := client.GetMap(context.TODO(), "test")
	if getMMErr != nil {
		t.Error(getMMErr)
		return
	}
	setErr := mm.Set(context.TODO(), "some", []byte(time.Now().String()))
	if setErr != nil {
		t.Error(setErr)
		return
	}
	v, getErr := mm.Get(context.TODO(), "some")
	if getErr != nil {
		t.Error(getErr)
		return
	}
	t.Log(reflect.ValueOf(v).Type(), string(v.([]byte)))
	t.Log(mm.SetTTL(context.TODO(), "not", 1*time.Second))
	t.Log(mm.Get(context.TODO(), "not"))
}

func instance(id string, ver versions.Version, addr string) (cluster *hazelcasts.Cluster, err error) {
	log, logErr := logs.New(logs.WithLevel(logs.DebugLevel))
	if logErr != nil {
		err = logErr
		return
	}
	conf := configs.Config{
		Addr:     []string{"127.0.0.1:15701"},
		Username: "",
		Password: "",
		SSL:      configs.SSLConfig{},
		KeepAlive: configs.KeepAliveConfig{
			TTL:      10 * time.Second,
			Interval: 5 * time.Second,
		},
		Shared:  nil,
		Barrier: nil,
	}
	cp, _ := json.Marshal(conf)
	config, configErr := configures.NewJsonConfig(cp)
	if configErr != nil {
		err = configErr
		return
	}
	options := fc.ClusterOptions{
		Log:     log,
		Config:  config,
		Id:      id,
		Version: ver,
		Address: addr,
	}
	cluster = &hazelcasts.Cluster{}
	err = cluster.Construct(options)
	return
}

func TestCluster_A(t *testing.T) {
	cluster, clusterErr := instance("A", versions.New(1, 0, 0), "127.0.0.1:8081")
	if clusterErr != nil {
		t.Errorf("%+v", clusterErr)
		return
	}
	cluster.AddService(fc.Service{
		Name:        "s11",
		Internal:    false,
		Functions:   nil,
		DocumentRaw: nil,
	})

	cluster.AddService(fc.Service{
		Name:        "s12",
		Internal:    false,
		Functions:   nil,
		DocumentRaw: nil,
	})
	ctx := context.TODO()
	joinErr := cluster.Join(ctx)
	if joinErr != nil {
		t.Errorf("%+v", joinErr)
		return
	}
	for {
		event, ok := <-cluster.NodeEvents()
		if !ok {
			break
		}
		t.Log("A", event.Kind.String(), event.Node.Id)
	}
}

func TestCluster_B(t *testing.T) {
	cluster, clusterErr := instance("B", versions.New(1, 0, 0), "127.0.0.1:8082")
	if clusterErr != nil {
		t.Errorf("%+v", clusterErr)
		return
	}
	cluster.AddService(fc.Service{
		Name:        "s21",
		Internal:    false,
		Functions:   nil,
		DocumentRaw: nil,
	})

	cluster.AddService(fc.Service{
		Name:        "s22",
		Internal:    false,
		Functions:   nil,
		DocumentRaw: nil,
	})
	ctx := context.TODO()
	joinErr := cluster.Join(ctx)
	if joinErr != nil {
		t.Errorf("%+v", joinErr)
		return
	}
	for {
		event, ok := <-cluster.NodeEvents()
		if !ok {
			break
		}
		t.Log("B", event.Kind.String(), event.Node.Id)
	}
}

func TestCluster_C(t *testing.T) {
	cluster, clusterErr := instance("C", versions.New(1, 0, 0), "127.0.0.1:8083")
	if clusterErr != nil {
		t.Errorf("%+v", clusterErr)
		return
	}
	ctx := context.TODO()
	joinErr := cluster.Join(ctx)
	if joinErr != nil {
		t.Errorf("%+v", joinErr)
		return
	}
	for {
		event, ok := <-cluster.NodeEvents()
		if !ok {
			break
		}
		t.Log("C", event.Kind.String(), event.Node.Id)
	}
}
