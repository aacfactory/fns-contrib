package etcd

type Config struct {
	Endpoints          []string `json:"endpoints"`
	Username           string   `json:"username,omitempty"`
	Password           string   `json:"password,omitempty"`
	DialTimeoutSecond  int      `json:"dialTimeoutSecond,omitempty"`
	GrantTTLSecond     int      `json:"grantTtlSecond,omitempty"`
	SSL                bool     `json:"ssl,omitempty"`
	CaFilePath         string   `json:"caFilePath,omitempty"`
	CertFilePath       string   `json:"certFilePath,omitempty"`
	KeyFilePath        string   `json:"keyFilePath,omitempty"`
	InsecureSkipVerify bool     `json:"insecureSkipVerify,omitempty"`
}
