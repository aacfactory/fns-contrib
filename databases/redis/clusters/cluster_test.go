package clusters_test

import (
	"github.com/aacfactory/configures"
	"github.com/aacfactory/fns-contrib/databases/redis/clusters"
	"github.com/aacfactory/fns-contrib/databases/redis/configs"
	fc "github.com/aacfactory/fns/clusters"
	"github.com/aacfactory/fns/commons/versions"
	"github.com/aacfactory/fns/context"
	"github.com/aacfactory/json"
	"github.com/aacfactory/logs"
	"testing"
	"time"
)

func instance(id string, ver versions.Version, addr string) (cluster *clusters.Cluster, err error) {
	log, logErr := logs.New(logs.WithLevel(logs.DebugLevel))
	if logErr != nil {
		err = logErr
		return
	}
	conf := clusters.Config{
		Config: configs.Config{
			InitAddress: []string{"127.0.0.1:16379"},
		},
		KeepAlive: clusters.KeepAliveConfig{
			TTL:      10 * time.Second,
			Interval: 5 * time.Second,
		},
		Barrier: clusters.BarrierConfig{},
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
	cluster = &clusters.Cluster{}
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
