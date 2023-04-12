package tokens

import (
	"context"
	"github.com/aacfactory/configures"
	"github.com/aacfactory/errors"
	"github.com/aacfactory/fns/commons/versions"
	"github.com/aacfactory/fns/service"
	"github.com/aacfactory/logs"
	"time"
)

var (
	ErrTokenNotFound = errors.Warning("tokens: token was not found")
)

type SaveParam struct {
	Id       string    `json:"id"`
	UserId   string    `json:"userId"`
	Token    string    `json:"token"`
	ExpireAT time.Time `json:"expireAt"`
}

type RemoveParam struct {
	Id     string `json:"id"`
	UserId string `json:"userId"`
}

type Token struct {
	Id       string    `json:"id"`
	UserId   string    `json:"userId"`
	Token    string    `json:"token"`
	ExpireAT time.Time `json:"expireAt"`
}

type StoreOptions struct {
	AppId      string
	AppName    string
	AppVersion versions.Version
	Log        logs.Logger
	Config     configures.Config
}

type Store interface {
	Build(options StoreOptions) (err error)
	Save(ctx context.Context, param SaveParam) (err errors.CodeError)
	Remove(ctx context.Context, param RemoveParam) (err errors.CodeError)
	Get(ctx context.Context, id string) (token Token, has bool, err errors.CodeError)
	List(ctx context.Context, userId string) (tokens []Token, err errors.CodeError)
	Close()
}

const (
	storeComponentName = "store"
)

type storeComponent struct {
	store Store
}

func (component *storeComponent) Name() string {
	return storeComponentName
}

func (component *storeComponent) Build(options service.ComponentOptions) (err error) {
	err = component.store.Build(StoreOptions{
		AppId:      options.AppId,
		AppName:    options.AppName,
		AppVersion: options.AppVersion,
		Log:        options.Log,
		Config:     options.Config,
	})
	return
}

func (component *storeComponent) Close() {
	component.store.Close()
}

func convertStoreToComponent(store Store) service.Component {
	return &storeComponent{
		store: store,
	}
}
