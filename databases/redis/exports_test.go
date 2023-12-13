package redis_test

import (
	"fmt"
	"github.com/aacfactory/fns-contrib/databases/redis"
	"github.com/aacfactory/fns/tests"
	"testing"
)

func TestExport(t *testing.T) {
	setupErr := setup()
	if setupErr != nil {
		t.Error(fmt.Sprintf("%+v", setupErr))
		return
	}
	defer tests.Teardown()
	ctx := tests.TODO()
	client, err := redis.Export(ctx)
	if err != nil {
		t.Errorf("%+v", err)
		return
	}
	r, pingErr := client.Do(ctx, client.B().Ping().Build()).ToString()
	if pingErr != nil {
		t.Error(pingErr)
		return
	}
	t.Log(r)
}
