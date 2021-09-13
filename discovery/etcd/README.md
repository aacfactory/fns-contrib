# 概述
基于 ETCD 的 FNS 服务注册与发现。
## 安装
```shell
go get github.com/aacfactory/fns-contrib/discovery/etcd
```
## 使用
配置文件，其中kind必须是etcd，且必须小写。
```json
{
  "services": {
    "discovery": {
      "enable": true,
      "kind": "etcd",
      "config": {
        "endpoints": [
          ""
        ],
        "username": "",
        "password": "",
        "dialTimeoutSecond": 10,
        "grantTtlSecond": 10,
        "ssl": false,
        "caFilePath": "",
        "certFilePath": "",
        "keyFilePath": "",
        "insecureSkipVerify": false
      }
    }
  }
}
```
代码注入，没有其它操作了。
```go
import _ "github.com/aacfactory/fns-contrib/discovery/etcd"
```
