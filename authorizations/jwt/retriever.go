package jwt

import (
	"fmt"
	"github.com/aacfactory/configuares"
	"github.com/aacfactory/fns"
	gwt "github.com/golang-jwt/jwt/v4"
	"io/ioutil"
	"path/filepath"
	"strings"
	"time"
)

const (
	kind = "jwt"
)

func init() {
	fns.RegisterAuthorizationsRetriever(kind, Retriever)
}

func Retriever(raw configuares.Raw) (authorizations fns.Authorizations, err error) {

	config := Config{}
	configErr := raw.As(&config)
	if configErr != nil {
		err = fmt.Errorf("fns Authorizations JWT: read config failed, %v", configErr)
		return
	}

	methodName := strings.ToUpper(strings.TrimSpace(config.Method))

	method := gwt.GetSigningMethod(methodName)
	if method == nil {
		err = fmt.Errorf("fns Authorizations JWT: method is not support")
		return
	}

	sk := strings.TrimSpace(config.SK)
	pf := strings.TrimSpace(config.PublicKey)
	sf := strings.TrimSpace(config.PrivateKey)


	var pubKey interface{} = nil
	var priKey interface{} = nil


	switch method.(type) {
	case *gwt.SigningMethodHMAC:
		if sk == "" {
			sk = "+-fns"
		}
		pubKey = []byte(sk)
		priKey = []byte(sk)
	case *gwt.SigningMethodECDSA:
		pub, pri, readErr := readPairKeyPem(pf, sf)
		if readErr != nil {
			err= readErr
			return
		}
		var parseErr error
		pubKey, parseErr = gwt.ParseECPublicKeyFromPEM(pub)
		if parseErr != nil {
			err = fmt.Errorf("fns Authorizations JWT: parse ECPublicKey failed, %v", parseErr)
			return
		}
		priKey, parseErr = gwt.ParseECPrivateKeyFromPEM(pri)
		if parseErr != nil {
			err = fmt.Errorf("fns Authorizations JWT: parse ECPrivateKey failed, %v", parseErr)
			return
		}
	case *gwt.SigningMethodRSAPSS, *gwt.SigningMethodRSA:
		pub, pri, readErr := readPairKeyPem(pf, sf)
		if readErr != nil {
			err= readErr
			return
		}
		var parseErr error
		pubKey, parseErr = gwt.ParseRSAPublicKeyFromPEM(pub)
		if parseErr != nil {
			err = fmt.Errorf("fns Authorizations JWT: parse RSAPublicKey failed, %v", parseErr)
			return
		}
		priKey, parseErr = gwt.ParseRSAPrivateKeyFromPEM(pri)
		if parseErr != nil {
			err = fmt.Errorf("fns Authorizations JWT: parse RSAPrivateKey failed, %v", parseErr)
			return
		}
	case *gwt.SigningMethodEd25519:
		pub, pri, readErr := readPairKeyPem(pf, sf)
		if readErr != nil {
			err= readErr
			return
		}
		var parseErr error
		pubKey, parseErr = gwt.ParseEdPublicKeyFromPEM(pub)
		if parseErr != nil {
			err = fmt.Errorf("fns Authorizations JWT: parse EDPublicKey failed, %v", parseErr)
			return
		}
		priKey, parseErr = gwt.ParseEdPrivateKeyFromPEM(pri)
		if parseErr != nil {
			err = fmt.Errorf("fns Authorizations JWT: parse EDAPrivateKey failed, %v", parseErr)
			return
		}
	default:
		err = fmt.Errorf("fns Authorizations JWT: method is not support")
		return
	}

	issuer := strings.TrimSpace(config.Issuer)
	if issuer == "" {
		issuer = "FNS"
	}
	audience := ""
	if config.Audience != nil {
		for _, v := range config.Audience {
			audience = ", " + v
		}
		if len(audience) > 2 {
			audience = audience[2:]
		}
	}

	expirations := strings.TrimSpace(config.Expirations)

	if expirations == "" {
		expirations = "720h0m0s"
	}

	expires, expiresErr := time.ParseDuration(expirations)
	if expiresErr != nil {
		err = fmt.Errorf("fns Authorizations JWT: expirations is invalied, %v", expiresErr)
		return
	}

	store, storeErr := NewStore(config.Store)
	if storeErr != nil {
		err = fmt.Errorf("fns Authorizations JWT: create store failed, %v", storeErr)
		return
	}


	authorizations = &Authorizations{
		method:      method,
		pubKey:      pubKey,
		priKey:      priKey,
		issuer:      issuer,
		audience:    audience,
		expirations: expires,
		store:       store,
	}


	return
}

func readPairKeyPem(pf string, sf string) (pubPEM []byte, priPEM []byte, err error) {
	pubFile, pubFileErr := filepath.Abs(pf)
	if pubFileErr != nil {
		err = fmt.Errorf("fns Authorizations JWT: read pub key file failed, %v", pubFileErr)
		return
	}
	pubPEM0, pubReadErr := ioutil.ReadFile(pubFile)
	if pubReadErr != nil {
		err = fmt.Errorf("fns Authorizations JWT: read pub key file failed, %v", pubReadErr)
		return
	}
	pubPEM = pubPEM0

	priFile, priFileErr := filepath.Abs(sf)
	if priFileErr != nil {
		err = fmt.Errorf("fns Authorizations JWT: read pri key file failed, %v", priFileErr)
		return
	}
	priPEM0, priReadErr := ioutil.ReadFile(priFile)
	if priReadErr != nil {
		err = fmt.Errorf("fns Authorizations JWT: read pri key file failed, %v", priReadErr)
		return
	}
	priPEM = priPEM0
	return
}