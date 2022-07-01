# Docker swarm cluster bootstrap
## Install
```shell
go get github.com/aacfactory/fns-contrib/cluster/swarm
```
## Usage
```go
import (
	_ "github.com/aacfactory/fns-contrib/cluster/swarm"
)

```
Config
```yaml
cluster:
  devMode: false
  kind: "swarm"
  options:
    swarm:
      fromEnv: false
      host: "swarm master host"
      certDir: "cert file dir, there are [ca.pem, cert.pem, key.pem] in the dir"
      labels:
        - "FNS-SERVICE"
        - "ACTIVE=dev"       
```