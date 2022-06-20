package swarm

type Config struct {
	FromENV bool     `json:"fromEnv"`
	Host    string   `json:"host"`
	CertDir string   `json:"certDir"`
	Labels  []string `json:"labels"`
}
