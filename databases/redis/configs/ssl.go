package configs

import (
	"crypto/tls"
	"crypto/x509"
	"github.com/aacfactory/errors"
	"os"
	"path/filepath"
	"strings"
)

type SSLConfig struct {
	Enable             bool   `json:"enable"`
	CaFilePath         string `json:"caFilePath"`
	CertFilePath       string `json:"certFilePath"`
	KeyFilePath        string `json:"keyFilePath"`
	InsecureSkipVerify bool   `json:"insecureSkipVerify"`
}

func (ssl *SSLConfig) Config() (config *tls.Config, err error) {
	cas := x509.NewCertPool()
	if ssl.CaFilePath != "" {
		path := strings.TrimSpace(ssl.CaFilePath)
		if !filepath.IsAbs(path) {
			path, err = filepath.Abs(path)
			if err != nil {
				err = errors.Warning("get absolute representation of path failed").WithMeta("path", path).WithCause(err)
				return
			}
		}
		path = filepath.ToSlash(path)
		p, readErr := os.ReadFile(path)
		if readErr != nil {
			err = errors.Warning("read file failed").WithMeta("path", path).WithCause(readErr)
			return
		}
		cas.AppendCertsFromPEM(p)
	}
	cert := strings.TrimSpace(ssl.CertFilePath)
	if cert == "" {
		err = errors.Warning("cert file path is required")
		return
	}
	if !filepath.IsAbs(cert) {
		cert, err = filepath.Abs(cert)
		if err != nil {
			err = errors.Warning("get absolute representation of path failed").WithMeta("path", cert).WithCause(err)
			return
		}
	}
	cert = filepath.ToSlash(cert)
	certPEM, readCertErr := os.ReadFile(cert)
	if readCertErr != nil {
		err = errors.Warning("read file failed").WithMeta("path", cert).WithCause(readCertErr)
		return
	}
	key := strings.TrimSpace(ssl.KeyFilePath)
	if key == "" {
		err = errors.Warning("key file path is required")
		return
	}
	if !filepath.IsAbs(key) {
		key, err = filepath.Abs(key)
		if err != nil {
			err = errors.Warning("get absolute representation of path failed").WithMeta("path", key).WithCause(err)
			return
		}
	}
	key = filepath.ToSlash(key)
	keyPEM, readKeyErr := os.ReadFile(key)
	if readKeyErr != nil {
		err = errors.Warning("read file failed").WithMeta("path", key).WithCause(readKeyErr)
		return
	}
	certificate, certificateErr := tls.X509KeyPair(certPEM, keyPEM)
	if certificateErr != nil {
		err = errors.Warning("make x509 keypair failed").WithCause(certificateErr)
		return
	}
	config = &tls.Config{
		RootCAs:            cas,
		Certificates:       []tls.Certificate{certificate},
		InsecureSkipVerify: ssl.InsecureSkipVerify,
	}
	return
}
