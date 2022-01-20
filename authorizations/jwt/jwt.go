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
	audience    []string
	expirations time.Duration
}

func (auth *Authorizations) Encode(ctx fns.Context, claims interface{}) (value []byte, err errors.CodeError) {
	if claims == nil {
		err = errors.ServiceError("fns JWT: sign token failed for claims is nil")
		return
	}
	userClaims, typeOk := claims.(*UserClaims)
	if !typeOk {
		err = errors.ServiceError("fns JWT: sign token failed for claims is not *UserClaims")
		return
	}

	if !userClaims.attributes.Contains("id") {
		err = errors.ServiceError("fns JWT: sign token failed for claims has no user id")
		return
	}

	registeredClaims := newRegisteredClaims()
	registeredClaims.SetId(fns.UID())
	registeredClaims.SetAudience(auth.audience)
	registeredClaims.SetExpiresAt(time.Now().Add(auth.expirations))
	registeredClaims.SetIssuer(auth.issuer)
	registeredClaims.SetIssuerAt(time.Now())
	registeredClaims.SetNotBefore(time.Now())
	registeredClaims.SetSub(userClaims.sub)
	registeredClaims.Attr = userClaims.attributes

	token := gwt.NewWithClaims(auth.method, registeredClaims)

	signed, signErr := token.SignedString(auth.priKey)
	if signErr != nil {
		err = errors.ServiceError("fns JWT: sign token failed").WithCause(signErr)
		return
	}

	value = make([]byte, 7+len(signed))
	copy(value[:7], prefix)
	copy(value[7:], signed)
	ctx.User().SetAuthorization(value)
	registeredClaims.mapToUser(ctx.User())

	return
}

func (auth *Authorizations) Decode(ctx fns.Context, value []byte) (err errors.CodeError) {
	if value == nil || len(value) < 7 {
		err = errors.Unauthorized(fmt.Sprintf("fns JWT: decode failed for %s is not jwt", string(value)))
		return
	}

	if bytes.Index(value, prefix) != 0 {
		err = errors.Unauthorized(fmt.Sprintf("fns JWT: decode failed for %s is not jwt", string(value)))
		return
	}

	token, parseErr := gwt.ParseWithClaims(string(value[7:]), newRegisteredClaims(), func(token *gwt.Token) (interface{}, error) {
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

	claims, ok := token.Claims.(*jwtClaims)
	if !ok {
		err = errors.Unauthorized("fns JWT: decode failed for type of claims is not *jwt.jwtClaims")
		return
	}

	claims.mapToUser(ctx.User())

	return
}
