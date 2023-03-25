package tokens

import (
	"context"
	"github.com/aacfactory/errors"
	"github.com/aacfactory/fns/service"
	"time"
)

type SaveParam struct {
	Id          string        `json:"id"`
	UserId      string        `json:"userId"`
	Token       string        `json:"token"`
	Expirations time.Duration `json:"expirations"`
}

type RemoveParam struct {
	Id     string `json:"id"`
	UserId string `json:"userId"`
}

type Token struct {
	Id          string        `json:"id"`
	UserId      string        `json:"userId"`
	Token       string        `json:"token"`
	Expirations time.Duration `json:"expirations"`
}

type Store interface {
	service.Component
	Save(ctx context.Context, param SaveParam) (err errors.CodeError)
	Remove(ctx context.Context, param RemoveParam) (err errors.CodeError)
	Get(ctx context.Context, id string) (token Token, err errors.CodeError)
	List(ctx context.Context, userId string) (tokens []Token, err errors.CodeError)
}
