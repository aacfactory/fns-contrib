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
    kubernetes:
      inCluster: true
      kubeConfigPath: "~/.kube" # when inCluster is false
      namespace: "fns-dev"
      timeoutSeconds: 60
      labels:
        - "FNS=SERVICE"       
```
Note: env `MY_POD_NAME` and `MY_POD_IP` are required, see [inject-data-application](https://kubernetes.io/zh-cn/docs/tasks/inject-data-application/environment-variable-expose-pod-information/) for more.