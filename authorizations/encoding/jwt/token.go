package jwt

import (
	"github.com/aacfactory/json"
	gwt "github.com/golang-jwt/jwt/v4"
	"time"
)

type Token struct {
	Claims *Claims
	raw    []byte
}

func (t *Token) Id() (id string) {
	id = t.Claims.ID
	return
}

func (t *Token) NotBefore() (date time.Time) {
	date = t.Claims.IssuedAt.Time
	return
}

func (t *Token) NotAfter() (date time.Time) {
	date = t.Claims.ExpiresAt.Time
	return
}

func (t *Token) User() (id string, attr *json.Object) {
	id = t.Claims.Subject
	attr = t.Claims.Attr
	return
}

func (t *Token) Bytes() (p []byte) {
	p = t.raw
	return
}

type Claims struct {
	gwt.RegisteredClaims
	Attr *json.Object `json:"attr"`
}
