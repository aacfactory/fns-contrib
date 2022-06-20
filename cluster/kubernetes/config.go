package kubernetes

type Config struct {
	InCluster      bool     `json:"inCluster"`
	KubeConfigPath string   `json:"kubeConfigPath"`
	Namespace      string   `json:"namespace"`
	Labels         []string `json:"labels"`
	TimeoutSeconds int      `json:"timeoutSeconds"`
}
