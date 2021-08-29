package redis

import (
	"context"
	"fmt"
	rds "github.com/go-redis/redis/v8"
	"math/rand"
)

type Client interface {
	Do(ctx context.Context, args ...interface{}) *rds.Cmd
	Writer() (cmd rds.Cmdable)
	Reader() (cmd rds.Cmdable)
	Close() (err error)
}

type Standalone struct {
	client *rds.Client
}

func (client *Standalone) Do(ctx context.Context, args ...interface{}) *rds.Cmd {
	return client.client.Do(ctx, args)
}

func (client *Standalone) Writer() (cmd rds.Cmdable) {
	cmd = client.client
	return
}

func (client *Standalone) Reader() (cmd rds.Cmdable) {
	cmd = client.client
	return
}

func (client *Standalone) Close() (err error) {
	err = client.client.Close()
	return
}

type MasterSlaver struct {
	master     *rds.Client
	slavers    []*rds.Client
	slaversNum int
}

func (client *MasterSlaver) Do(ctx context.Context, args ...interface{}) *rds.Cmd {
	return client.master.Do(ctx, args)
}

func (client *MasterSlaver) Writer() (cmd rds.Cmdable) {
	cmd = client.master
	return
}

func (client *MasterSlaver) Reader() (cmd rds.Cmdable) {
	cmd = client.slavers[rand.Intn(client.slaversNum)]
	return
}

func (client *MasterSlaver) Close() (err error) {
	errs := ""

	masterCloseErr := client.master.Close()
	if masterCloseErr != nil {
		errs = errs + ", " + masterCloseErr.Error()
	}

	for _, slaver := range client.slavers {
		slaverCloseErr := slaver.Close()
		if slaverCloseErr != nil {
			errs = errs + ", " + slaverCloseErr.Error()
		}
	}

	if errs != "" {
		err = fmt.Errorf(errs[2:])
		return
	}
	return
}

type Cluster struct {
	client *rds.ClusterClient
}

func (client *Cluster) Do(ctx context.Context, args ...interface{}) *rds.Cmd {
	return client.client.Do(ctx, args)
}

func (client *Cluster) Writer() (cmd rds.Cmdable) {
	cmd = client.client
	return
}

func (client *Cluster) Reader() (cmd rds.Cmdable) {
	cmd = client.client
	return
}

func (client *Cluster) Close() (err error) {
	err = client.client.Close()
	return
}
