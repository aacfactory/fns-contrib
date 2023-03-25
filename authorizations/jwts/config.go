package jwts

import (
	"fmt"
	"github.com/aacfactory/errors"
	"github.com/golang-jwt/jwt/v4"
	"os"
	"path/filepath"
	"strings"
)

type Config struct {
	Method     string   `json:"method"`
	SK         string   `json:"sk"`
	PublicKey  string   `json:"publicKey"`
	PrivateKey string   `json:"privateKey"`
	Issuer     string   `json:"issuer"`
	Audience   []string `json:"audience"`
}

func (config *Config) CreateJWT() (v *JWT, err error) {
	methodName := strings.ToUpper(strings.TrimSpace(config.Method))
	method := jwt.GetSigningMethod(methodName)
	if method == nil {
		err = errors.Warning("jwt: build tokens failed").WithCause(errors.Warning("jwt: method is not support").WithMeta("method", methodName))
		return
	}
	sk := strings.TrimSpace(config.SK)
	pf := strings.TrimSpace(config.PublicKey)
	sf := strings.TrimSpace(config.PrivateKey)
	var pubKey interface{} = nil
	var priKey interface{} = nil
	switch method.(type) {
	case *jwt.SigningMethodHMAC:
		if sk == "" {
			err = errors.Warning("jwt: build tokens failed").WithCause(errors.Warning("jwt: HMAC method require sk"))
			return
		}
		pubKey = []byte(sk)
		priKey = []byte(sk)
	case *jwt.SigningMethodECDSA:
		pubPEM, readPubPEMErr := readPEM(pf)
		if readPubPEMErr != nil {
			err = errors.Warning("jwt: build tokens failed").WithCause(readPubPEMErr)
			return
		}
		pubKey, err = jwt.ParseECPublicKeyFromPEM(pubPEM)
		if err != nil {
			err = errors.Warning("jwt: build tokens failed").WithCause(err)
			return
		}
		priPEM, readPriPEMErr := readPEM(sf)
		if readPriPEMErr != nil {
			err = errors.Warning("jwt: build tokens failed").WithCause(readPriPEMErr)
			return
		}
		priKey, err = jwt.ParseECPrivateKeyFromPEM(priPEM)
		if err != nil {
			err = errors.Warning("jwt: build tokens failed").WithCause(readPriPEMErr)
			return
		}
	case *jwt.SigningMethodRSAPSS, *jwt.SigningMethodRSA:
		pubPEM, readPubPEMErr := readPEM(pf)
		if readPubPEMErr != nil {
			err = errors.Warning("jwt: build tokens failed").WithCause(readPubPEMErr)
			return
		}
		pubKey, err = jwt.ParseRSAPublicKeyFromPEM(pubPEM)
		if err != nil {
			err = errors.Warning("jwt: build tokens failed").WithCause(err)
			return
		}
		priPEM, readPriPEMErr := readPEM(sf)
		if readPriPEMErr != nil {
			err = errors.Warning("jwt: build tokens failed").WithCause(readPriPEMErr)
			return
		}
		priKey, err = jwt.ParseRSAPrivateKeyFromPEM(priPEM)
		if err != nil {
			err = errors.Warning("jwt: build tokens failed").WithCause(readPriPEMErr)
			return
		}
	case *jwt.SigningMethodEd25519:
		pubPEM, readPubPEMErr := readPEM(pf)
		if readPubPEMErr != nil {
			err = errors.Warning("jwt: build tokens failed").WithCause(readPubPEMErr)
			return
		}
		pubKey, err = jwt.ParseEdPublicKeyFromPEM(pubPEM)
		if err != nil {
			err = errors.Warning("jwt: build tokens failed").WithCause(err)
			return
		}
		priPEM, readPriPEMErr := readPEM(sf)
		if readPriPEMErr != nil {
			err = errors.Warning("jwt: build tokens failed").WithCause(readPriPEMErr)
			return
		}
		priKey, err = jwt.ParseEdPrivateKeyFromPEM(priPEM)
		if err != nil {
			err = errors.Warning("jwt: build tokens failed").WithCause(readPriPEMErr)
			return
		}
	default:
		err = errors.Warning("jwt: build tokens failed").WithCause(fmt.Errorf("method is not support"))
		return
	}
	issuer := strings.TrimSpace(config.Issuer)
	if issuer == "" {
		issuer = "FNS"
	}
	audience := make([]string, 0, 1)
	if config.Audience != nil {
		audience = config.Audience
	}
	v = &JWT{
		method:   method,
		pubKey:   pubKey,
		priKey:   priKey,
		issuer:   issuer,
		audience: audience,
	}
	return
}

func readPEM(path string) (pem []byte, err error) {
	if !filepath.IsAbs(path) {
		path, err = filepath.Abs(path)
		if err != nil {
			err = errors.Warning("read pem failed").WithCause(err).WithMeta("path", path)
			return
		}
	}
	path = filepath.ToSlash(path)
	pem, err = os.ReadFile(path)
	if err != nil {
		err = errors.Warning("read pem failed").WithCause(err).WithMeta("path", path)
		return
	}

	return
}
