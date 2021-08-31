package etcd_test

import (
	"encoding/json"
	"fmt"
	"github.com/aacfactory/configuares"
	"github.com/aacfactory/errors"
	"github.com/aacfactory/fns"
	"github.com/aacfactory/fns-contrib/discovery/etcd"
	"reflect"
	"runtime"
	"testing"
	"time"
)

func Test_Example(t *testing.T) {

	runtime.GOMAXPROCS(16)

	now := time.Now()

	da, daErr := d1()
	if daErr != nil {
		t.Error(daErr)
		return
	}
	fmt.Println("new a", time.Now().Sub(now))

	now = time.Now()
	aPubErr := da.Publish(&FakeService{
		namespace: "users",
	})
	fmt.Println("pub a", time.Now().Sub(now))

	if aPubErr != nil {
		t.Error(aPubErr)
		return
	}

	now = time.Now()
	db, dbErr := d2()
	if dbErr != nil {
		t.Error(dbErr)
		return
	}
	fmt.Println("new b", time.Now().Sub(now))

	fmt.Println("start", time.Now().Format(time.RFC3339))

	now = time.Now()
	bPubErr := db.Publish(&FakeService{
		namespace: "hello",
	})
	fmt.Println("pub b", time.Now().Sub(now))

	if bPubErr != nil {
		t.Error(bPubErr)
		return
	}

	//
	now = time.Now()
	up1, up1Err := da.Proxy(nil, "users")
	if up1Err != nil {
		t.Error(up1Err)
	}
	fmt.Println("ok", reflect.TypeOf(up1), time.Now().Sub(now))

	now = time.Now()
	up2, up2Err := db.Proxy(nil,"users")
	if up2Err != nil {
		t.Error(up2Err)
	}
	fmt.Println("ok", reflect.TypeOf(up2), time.Now().Sub(now))

	//
	da.Close()

	now = time.Now()
	_, up3Err := db.Proxy(nil,"users")
	if up3Err != nil {
		fmt.Println("ko", up3Err)
	}
	fmt.Println("remote", time.Now().Sub(now))

	fmt.Println("end", time.Now().Format(time.RFC3339))

}

func d1() (discovery fns.ServiceDiscovery, err error) {
	config := etcd.Config{
		Endpoints: []string{"127.0.0.1:2379"},
	}

	p, _ := json.Marshal(config)

	option := fns.ServiceDiscoveryOption{
		ServerId: "foo",
		Address:  "127.0.0.1:8080",
		Config:   configuares.Raw(p),
	}

	discovery, err = etcd.Retriever(option)

	return
}

func d2() (discovery fns.ServiceDiscovery, err error) {
	config := etcd.Config{
		Endpoints: []string{"127.0.0.1:2379"},
	}

	p, _ := json.Marshal(config)

	option := fns.ServiceDiscoveryOption{
		ServerId: "bar",
		Address:  "127.0.0.1:8080",
		Config:   configuares.Raw(p),
	}

	discovery, err = etcd.Retriever(option)

	return
}

type FakeService struct {
	namespace string
}

func (svc *FakeService) Namespace() string {
	return svc.namespace
}

func (svc *FakeService) Internal() bool {
	return false
}

func (svc *FakeService) Build(config configuares.Config) (err error) {

	return
}

func (svc *FakeService) Description() (description []byte) {
	return
}

func (svc *FakeService) Handle(context fns.Context, fn string, argument fns.Argument) (result interface{}, err errors.CodeError) {

	err = errors.NotFound(fmt.Sprintf("%s was not found in %s", fn, svc.Namespace()))
	return
}

func (svc *FakeService) Close() (err error) {

	return
}

type Ex struct {
	M map[string]string
}

func Test_MapPtr(t *testing.T) {

	m := make(map[string]*Ex)

	for i := 0; i < 3; i++ {
		m[fmt.Sprintf("%d", i)] = &Ex{
			M: map[string]string{"a":"b"},
		}
	}

	m["1"].M["a"] = "a"
	m["1"].M["b"] = "b"

	fmt.Println(m["1"])

}