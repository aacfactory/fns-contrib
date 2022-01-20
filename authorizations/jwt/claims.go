package jwt

import (
	"fmt"
	"github.com/aacfactory/fns"
	"github.com/aacfactory/json"
	gwt "github.com/golang-jwt/jwt/v4"
	"time"
)

func NewUserClaims() *UserClaims {
	return &UserClaims{
		sub:        "",
		attributes: json.NewObject(),
	}
}

type UserClaims struct {
	sub        string
	attributes *json.Object
}

func (u *UserClaims) SetIntUserId(v int) {
	_ = u.attributes.Put("id", v)
}

func (u *UserClaims) SetUserId(v string) {
	_ = u.attributes.Put("id", v)
}

func (u *UserClaims) SetSub(v string) {
	u.sub = v
}

func (u *UserClaims) Attributes() *json.Object {
	return u.attributes
}

func newRegisteredClaims() *jwtClaims {
	return &jwtClaims{
		RegisteredClaims: gwt.RegisteredClaims{},
		Attr:             json.NewObject(),
	}
}

type jwtClaims struct {
	gwt.RegisteredClaims
	Attr *json.Object `json:"attr,omitempty"`
}

func (c *jwtClaims) Valid() (err error) {
	err = c.Valid()
	if err != nil {
		return
	}

	if !c.Attr.Contains("id") {
		err = fmt.Errorf("id of attr in token was not found")
		return
	}

	return
}

func (c *jwtClaims) SetAudience(value []string) {
	c.Audience = value
}

func (c *jwtClaims) SetExpiresAt(value time.Time) {
	c.ExpiresAt = gwt.NewNumericDate(value)
}

func (c *jwtClaims) SetId(value string) {
	c.ID = value
}

func (c *jwtClaims) SetIssuer(value string) {
	c.Issuer = value
}

func (c *jwtClaims) SetIssuerAt(value time.Time) {
	c.IssuedAt = gwt.NewNumericDate(value)
}

func (c *jwtClaims) SetNotBefore(value time.Time) {
	c.NotBefore = gwt.NewNumericDate(value)
}

func (c *jwtClaims) SetSub(sub string) {
	c.Subject = sub
}

func (c *jwtClaims) mapToUser(user fns.User) {
	_ = user.Principals().Put("iss", c.Issuer)
	_ = user.Principals().Put("iat", c.IssuedAt)
	_ = user.Principals().Put("sub", c.Subject)
	_ = user.Principals().Put("aud", c.Audience)
	_ = user.Principals().Put("nbf", c.NotBefore)
	_ = user.Principals().Put("exp", c.ExpiresAt)
	_ = user.Principals().Put("jti", c.ID)
	_ = c.Attr.WriteTo(user.Attributes())
}
