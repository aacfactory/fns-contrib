package tokens

import (
	"context"
	"fmt"
	"github.com/aacfactory/errors"
	"github.com/aacfactory/fns/service"
	"time"
)

var (
	ErrTokenNotFound = fmt.Errorf("token was not found")
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

type Store interface {
	service.Component
	Save(ctx context.Context, param SaveParam) (err errors.CodeError)
	Remove(ctx context.Context, param RemoveParam) (err errors.CodeError)
	Get(ctx context.Context, id string) (token Token, err errors.CodeError)
	List(ctx context.Context, userId string) (tokens []Token, err errors.CodeError)
}
