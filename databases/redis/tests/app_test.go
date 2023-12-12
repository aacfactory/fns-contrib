package tests_test

import (
	"context"
	"github.com/redis/rueidis"
	"testing"
)

func TestConnect(t *testing.T) {
	client, err := rueidis.NewClient(rueidis.ClientOption{InitAddress: []string{"127.0.0.1:16379"}})
	if err != nil {
		panic(err)
	}
	defer client.Close()

	ctx := context.Background()
	// SET key val NX
	err = client.Do(ctx, client.B().Set().Key("key").Value("val").Nx().Build()).Error()
	// HGETALL hm
	v, err := client.Do(ctx, client.B().Get().Key("key").Build()).ToString()
	if err != nil {
		t.Error(err)
		return
	}

	t.Log(v)
}
