# Kubernetes
基于 Labels 获取 Service 的服务注册与发现。
## 安装
```go
go get github.com/aacfactory/fns-contrib/discovery/kubernetes
```
## 使用
配置文件，其中kind必须是 kubernetes，且必须小写。
```json
{
  "services": {
    "discovery": {
      "enable": true,
      "kind": "kubernetes",
      "config": {
        "namespace": "",      // kubernetes 的 namespace
        "checkingTimer": ""   // 健康检测轮询时间，默认 1m0s
      }
    }
  }
}
```
代码注入，没有其它操作了。
```go
import _ "github.com/aacfactory/fns-contrib/discovery/kubernetes"
```

## 注意
* 在 service 的 yaml 文件中需配置 key 为 fns，value 为 service namespace （多个时用 `,` 连接）的 Labels。
* 所有的 fns 必须是 clusterIP 类型。

