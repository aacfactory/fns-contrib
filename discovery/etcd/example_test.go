package etcd_test

import (
	"encoding/json"
	"fmt"
	"github.com/aacfactory/configuares"
	"github.com/aacfactory/errors"
	"github.com/aacfactory/fns"
	"github.com/aacfactory/fns-contrib/discovery/etcd"
	"reflect"
	"testing"
)

func Test_Example(t *testing.T) {

	da, daErr := d1()
	if daErr != nil {
		t.Error(daErr)
		return
	}

	aPubErr := da.Publish(&FakeService{
		namespace: "users",
	})

	if aPubErr != nil {
		t.Error(aPubErr)
		return
	}

	db, dbErr := d2()
	if dbErr != nil {
		t.Error(dbErr)
		return
	}

	bPubErr := db.Publish(&FakeService{
		namespace: "hello",
	})

	if bPubErr != nil {
		t.Error(bPubErr)
		return
	}

	//
	up1, up1Err := da.Proxy("users")
	if up1Err != nil {
		t.Error(up1Err)
		return
	}
	t.Log("ok", reflect.TypeOf(up1))

	up2, up2Err := db.Proxy("users")
	if up2Err != nil {
		t.Error(up2Err)
		return
	}
	t.Log("ok", reflect.TypeOf(up2))

	//
	da.Close()

	up3, up3Err := db.Proxy("users")
	if up3Err != nil {
		t.Log("ko", up3Err)
		return
	}
	t.Log(reflect.TypeOf(up3))

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
		Address:  "192.168.31.100:8080",
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
