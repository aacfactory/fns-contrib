package documents

import (
	"crypto/x509"
	"encoding/pem"
	"github.com/aacfactory/afssl"
	"github.com/aacfactory/errors"
	"os"
)

type Config struct {
	Enable  bool     `json:"enable"`
	Servers []Server `json:"servers"`
	OpenAPI OpenAPI  `json:"oas"`
}

type Server struct {
	Name string `json:"name"`
	URL  string `json:"url"`
	SSL  SSL    `json:"ssl"`
}

type OpenAPI struct {
	Version     string `json:"version"`
	Title       string `json:"title"`
	Description string `json:"description"`
	Term        string `json:"term"`
}

type SSL struct {
	CA                 string `json:"ca"`
	CAKey              string `json:"caKey"`
	Cert               string `json:"cert"`
	Key                string `json:"key"`
	InsecureSkipVerify bool   `json:"insecureSkipVerify"`
}

func (ssl *SSL) Read() (ok bool, err error) {
	if ssl.CA == "" && ssl.Cert == "" {
		return
	}
	if ssl.CA != "" {
		p, readErr := os.ReadFile(ssl.CA)
		if readErr != nil {
			err = errors.Warning("documents: read ca file failed").WithCause(readErr).WithMeta("file", ssl.CA)
			return
		}
		ssl.CA = string(p)
	}
	if ssl.CAKey != "" {
		p, readErr := os.ReadFile(ssl.CAKey)
		if readErr != nil {
			err = errors.Warning("documents: read ca key file failed").WithCause(readErr).WithMeta("file", ssl.CAKey)
			return
		}
		ssl.CAKey = string(p)
	}
	if ssl.Cert != "" {
		p, readErr := os.ReadFile(ssl.Cert)
		if readErr != nil {
			err = errors.Warning("documents: read cert file failed").WithCause(readErr).WithMeta("file", ssl.Cert)
			return
		}
		ssl.Cert = string(p)
	}
	if ssl.Key != "" {
		p, readErr := os.ReadFile(ssl.Key)
		if readErr != nil {
			err = errors.Warning("documents: read key file failed").WithCause(readErr).WithMeta("file", ssl.Key)
			return
		}
		ssl.Key = string(p)
	}
	if ssl.CA != "" && ssl.CAKey != "" && ssl.Cert == "" {
		// ssc
		caPEM := []byte(ssl.CA)
		caKeyPEM := []byte(ssl.CAKey)
		block, _ := pem.Decode(caPEM)
		ca, parseCaErr := x509.ParseCertificate(block.Bytes)
		if parseCaErr != nil {
			err = errors.Warning("documents: parse ca failed").WithCause(parseCaErr)
			return
		}
		sscConfig := afssl.CertificateConfig{
			Subject: &afssl.CertificatePkixName{
				Country:            ca.Subject.Country[0],
				Province:           ca.Subject.Province[0],
				Locality:           ca.Subject.Locality[0],
				Organization:       ca.Subject.Organization[0],
				OrganizationalUnit: ca.Subject.OrganizationalUnit[0],
				CommonName:         ca.Subject.CommonName,
			},
			IPs:      nil,
			Emails:   nil,
			DNSNames: nil,
		}
		clientCert, clientKey, createClientErr := afssl.GenerateCertificate(sscConfig, afssl.WithParent(caPEM, caKeyPEM), afssl.WithExpirationDays(365))
		if createClientErr != nil {
			err = errors.Warning("documents: create keypair by ca failed").WithCause(createClientErr)
			return
		}
		ssl.Cert = string(clientCert)
		ssl.Key = string(clientKey)
	}
	return
}
