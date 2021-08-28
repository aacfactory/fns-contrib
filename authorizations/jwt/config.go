package jwt

type Config struct {
	Method      string      `json:"method,omitempty"`
	SK          string      `json:"sk,omitempty"`
	PublicKey   string      `json:"publicKey,omitempty"`
	PrivateKey  string      `json:"privateKey,omitempty"`
	Issuer      string      `json:"issuer,omitempty"`
	Audience    []string    `json:"audience,omitempty"`
	Expirations string      `json:"expirations,omitempty"`
	Store       StoreConfig `json:"store,omitempty"`
}

type StoreConfig struct {
	Kind          string `json:"kind,omitempty"`
	Namespace     string `json:"namespace,omitempty"`
	ActiveTokenFn string `json:"activeTokenFn,omitempty"`
	LookUpTokenFn string `json:"lookUpTokenFn,omitempty"`
	RevokeTokenFn string `json:"revokeTokenFn,omitempty"`
}
