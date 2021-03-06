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

type masterSlaver struct {
	master     *rds.Client
	slavers    []*rds.Client
	slaversNum int
}

func (client *masterSlaver) Do(ctx context.Context, args ...interface{}) *rds.Cmd {
	return client.master.Do(ctx, args)
}

func (client *masterSlaver) Writer() (cmd rds.Cmdable) {
	cmd = client.master
	return
}

func (client *masterSlaver) Reader() (cmd rds.Cmdable) {
	cmd = client.slavers[rand.Intn(client.slaversNum)]
	return
}

func (client *masterSlaver) Close() (err error) {
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
