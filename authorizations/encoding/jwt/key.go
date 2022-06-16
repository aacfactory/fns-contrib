package jwt

import (
	"fmt"
	"io/ioutil"
	"path/filepath"
)

func readPairKeyPem(pf string, sf string) (pubPEM []byte, priPEM []byte, err error) {
	pubFile, pubFileErr := filepath.Abs(pf)
	if pubFileErr != nil {
		err = fmt.Errorf("jwt: read pub key file failed, %v", pubFileErr)
		return
	}
	pubPEM0, pubReadErr := ioutil.ReadFile(pubFile)
	if pubReadErr != nil {
		err = fmt.Errorf("jwt: read pub key file failed, %v", pubReadErr)
		return
	}
	pubPEM = pubPEM0
	priFile, priFileErr := filepath.Abs(sf)
	if priFileErr != nil {
		err = fmt.Errorf("jwt: read pri key file failed, %v", priFileErr)
		return
	}
	priPEM0, priReadErr := ioutil.ReadFile(priFile)
	if priReadErr != nil {
		err = fmt.Errorf("jwt: read pri key file failed, %v", priReadErr)
		return
	}
	priPEM = priPEM0
	return
}
