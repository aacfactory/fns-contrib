package kubernetes

type Config struct {
	Namespace string `json:"namespace,omitempty"`
	CheckingTimer string `json:"checkingTimer,omitempty"`
}
