package jwts

import (
	"github.com/golang-jwt/jwt/v4"
	"time"
)

type JWT struct {
	method      jwt.SigningMethod
	pubKey      interface{}
	priKey      interface{}
	issuer      string
	audience    []string
	expirations time.Duration
}

func (j *JWT) Sign(claims jwt.Claims) (signed string, err error) {
	jwtToken := jwt.NewWithClaims(j.method, claims)
	signed, err = jwtToken.SignedString(j.priKey)
	return
}

func (j *JWT) Parse(signed string) (err error) {
	return
}
