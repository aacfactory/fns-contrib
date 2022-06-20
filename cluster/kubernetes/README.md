# Kubernetes cluster bootstrap
## Install
```shell
go get github.com/aacfactory/fns-contrib/cluster/kubernetes
```
## Usage
```go
import (
	_ "github.com/aacfactory/fns-contrib/cluster/kubernetes"
)

```
Config
```yaml
cluster:
  devMode: false
  kind: "kubernetes"
  options:
    inCluster: true
    kubeConfigPath: "~/.kube" # when inCluster is false
    namespace: "fns-dev"
    timeoutSeconds: 60
    labels:
      - "FNS=SERVICE"       
```