package jwt

import (
	"bytes"
	"fmt"
	"github.com/aacfactory/errors"
	"github.com/aacfactory/fns"
	gwt "github.com/golang-jwt/jwt/v4"
	"time"
)

var (
	prefix = []byte("Bearer ")
)

type Authorizations struct {
	method      gwt.SigningMethod
	pubKey      interface{}
	priKey      interface{}
	issuer      string
	audience    string
	expirations time.Duration
	store       Store
}

func (auth *Authorizations) Encode(user fns.User) (value []byte, err error) {

	claims := NewUserClaims()
	claims.SetId(fns.UID())
	claims.SetAudience(auth.audience)
	claims.SetExpiresAt(time.Now().Add(auth.expirations))
	claims.SetIssuer(auth.issuer)
	claims.SetIssuerAt(time.Now())
	claims.SetNotBefore(time.Now())
	claims.SetSub(user.Id())

	token := gwt.NewWithClaims(auth.method, claims)

	signed, signErr := token.SignedString(auth.priKey)
	if signErr != nil {
		err = errors.ServiceError("fns UserClaims Encode: sign token failed").WithCause(signErr)
		return
	}

	claims.MapToUserPrincipals(user)

	value = make([]byte, 9+len(signed))
	copy(value[:9], prefix)
	copy(value[9:], signed)

	return
}

func (auth *Authorizations) Decode(value []byte, user fns.User) (err error) {

	if value == nil || len(value) < 9 {
		err = errors.Unauthorized(fmt.Sprintf("fns JWT Decode: %s is not jwt", string(value)))
		return
	}

	if bytes.Index(value, prefix) != 0 {
		err = errors.Unauthorized(fmt.Sprintf("fns JWT Decode: %s is not jwt", string(value)))
		return
	}

	token, parseErr := gwt.ParseWithClaims(string(value[9:]), &UserClaims{}, func(token *gwt.Token) (interface{}, error) {
		return auth.pubKey, nil
	})

	if parseErr != nil {
		validationError, ok := parseErr.(*gwt.ValidationError)
		if !ok {
			err = errors.Unauthorized(parseErr.Error())
			return
		}
		if validationError.Errors == gwt.ValidationErrorId {
			err = errors.Unauthorized("jti of token is invalid")
		} else if validationError.Errors == gwt.ValidationErrorAudience {
			err = errors.Unauthorized("aud of token is invalid")
		} else if validationError.Errors == gwt.ValidationErrorExpired {
			err = errors.Unauthorized("exp of token is invalid")
		} else if validationError.Errors == gwt.ValidationErrorIssuedAt {
			err = errors.Unauthorized("iat of token is invalid")
		} else if validationError.Errors == gwt.ValidationErrorMalformed {
			err = errors.Unauthorized("token is malformed")
		} else if validationError.Errors == gwt.ValidationErrorIssuer {
			err = errors.Unauthorized("iss of token is invalid")
		} else if validationError.Errors == gwt.ValidationErrorUnverifiable {
			err = errors.Unauthorized("token could not be verified because of signing problems")
		} else if validationError.Errors == gwt.ValidationErrorClaimsInvalid {
			err = errors.Unauthorized("Generic claims validation error")
		} else if validationError.Errors == gwt.ValidationErrorNotValidYet {
			err = errors.Unauthorized("nbf of token is invalid")
		} else if validationError.Errors == gwt.ValidationErrorSignatureInvalid {
			err = errors.Unauthorized("signature validation failed")
		} else {
			err = errors.Unauthorized(validationError.Error())
		}
		return
	}

	claims, ok := token.Claims.(*UserClaims)
	if !ok {
		err = errors.Unauthorized("type of claims is not *jwt.UserClaims")
		return
	}

	claims.MapToUserPrincipals(user)
	_ = user.Attributes().UnmarshalJSON(claims.Attr.Raw())

	return
}

func (auth *Authorizations) IsActive(ctx fns.Context, user fns.User) (ok bool) {
	id := ""
	_ = user.Principals().Get("jti", &id)
	if id == "" {
		return
	}

	ok = auth.store.LookUp(ctx, id)
	return
}

func (auth *Authorizations) Active(ctx fns.Context, user fns.User) (err error) {
	id := ""
	_ = user.Principals().Get("jti", &id)
	if id == "" {
		return
	}

	exp := int64(0)
	_ = user.Principals().Get("exp", &exp)

	if exp == 0 {
		exp = int64(auth.expirations)
		_ = user.Principals().Put("exp", time.Now().Add(auth.expirations))
	} else {
		exp = int64(time.Unix(exp, 0).Sub(time.Now()))
	}

	err = auth.store.Active(ctx, id, time.Duration(exp))

	return
}

func (auth *Authorizations) Revoke(ctx fns.Context, user fns.User) (err error) {
	id := ""
	_ = user.Principals().Get("jti", &id)
	if id == "" {
		return
	}

	err = auth.store.Revoke(ctx, id)
	return
}
