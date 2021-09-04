# REDIS

基于 fns.Service 实现的内部 Redis 服务。

## 安装

```go
go get github.com/aacfactory/fns-contrib/databases/redis
```

## 使用

配置文件

* 单机
    * masterSlaverMode = false，addr 列表为一个元素。
* 主从
    * masterSlaverMode = true，addr 列表第一个元素为主服务地址，后续为从服务地址。
* 集群
    * masterSlaverMode = false，addr 列表多元素。

```json
{
  "redis": {
    "masterSlaverMode": false,
    "network": "tcp",
    "addr": [
      "ip:port"
    ],
    "username": "",
    "password": "",
    "db": 0,
    "poolSize": 0,
    "ssl": false,
    "caFilePath": "",
    "certFilePath": "",
    "keyFilePath": "",
    "insecureSkipVerify": false
  }
}
```

服务部署

* fns为单机模式
    * 直接部署
* fns为分布式模式
    * 可以单独起一个（一组）只有 redis 服务的应用（推荐）。
    * 也可以与fns单机模式一样使用。

```go
app.Deply(redis.Service())
```

服务使用，具体参见 [github.com/aacfactory/fns-contrib/databases/redis/proxy.go](https://github.com/aacfactory/fns-contrib/tree/main/databases/redis/proxy.go)

```go
// get
result, err := redis.Get(ctx, key)
// set
err := redis.Set(ctx, &redis.SetParam{})


```

## 注意事项

* 它是一个内部服务，即只能被fn访问。
* 该服务主要服务于缓存，故只实现 key、strings、sorted set 的部分功能，如需要其它功能，请使用 Do 函数。
* 它是由 go-redis 实现，其 Do 函数是代理的 go-redis 中的 Do 函数，参数与返回值请参见 go-redis。
