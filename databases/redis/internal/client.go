package internal

import (
	"context"
	"fmt"
	"github.com/aacfactory/fns/commons/container/ring"
	rds "github.com/go-redis/redis/v8"
	"strings"
)

type Client interface {
	Do(ctx context.Context, args ...interface{}) *rds.Cmd
	Writer() (cmd rds.Cmdable)
	Reader() (cmd rds.Cmdable)
	Close() (err error)
}

type standalone struct {
	client *rds.Client
}

func (client *standalone) Do(ctx context.Context, args ...interface{}) *rds.Cmd {
	return client.client.Do(ctx, args)
}

func (client *standalone) Writer() (cmd rds.Cmdable) {
	cmd = client.client
	return
}

func (client *standalone) Reader() (cmd rds.Cmdable) {
	cmd = client.client
	return
}

func (client *standalone) Close() (err error) {
	err = client.client.Close()
	return
}

type keyedClient struct {
	key string
	v   *rds.Client
}

func (k *keyedClient) Key() string {
	return k.key
}

type masterSlaver struct {
	master  *rds.Client
	slavers *ring.Ring
}

func (client *masterSlaver) Do(ctx context.Context, args ...interface{}) *rds.Cmd {
	return client.master.Do(ctx, args)
}

func (client *masterSlaver) Writer() (cmd rds.Cmdable) {
	cmd = client.master
	return
}

func (client *masterSlaver) Reader() (cmd rds.Cmdable) {
	x := client.slavers.Next()
	if x == nil {
		return
	}
	kdb, _ := x.(*keyedClient)
	cmd = kdb.v
	return
}

func (client *masterSlaver) Close() (err error) {
	closeErrors := make([]string, 0, 1)
	masterCloseErr := client.master.Close()
	if masterCloseErr != nil {
		closeErrors = append(closeErrors, masterCloseErr.Error())
	}
	for i := 0; i < client.slavers.Size(); i++ {
		x := client.slavers.Next()
		if x == nil {
			return
		}
		kdb, _ := x.(*keyedClient)
		slaverErr := kdb.v.Close()
		if slaverErr != nil {
			closeErrors = append(closeErrors, slaverErr.Error())
		}
	}

	if len(closeErrors) > 0 {
		err = fmt.Errorf("redis: close failed, %v", strings.Join(closeErrors, ","))
		return
	}
	return
}

type cluster struct {
	client *rds.ClusterClient
}

func (client *cluster) Do(ctx context.Context, args ...interface{}) *rds.Cmd {
	return client.client.Do(ctx, args)
}

func (client *cluster) Writer() (cmd rds.Cmdable) {
	cmd = client.client
	return
}

func (client *cluster) Reader() (cmd rds.Cmdable) {
	cmd = client.client
	return
}

func (client *cluster) Close() (err error) {
	err = client.client.Close()
	return
}
