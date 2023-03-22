# REDIS

One redis service for fns

## Install

```shell
go get github.com/aacfactory/fns-contrib/databases/redis
```

## Usage

### Config

* Standalone type
    * addr size is one
* Cluster type
    * all in addr is members

Example
```yaml
redis:
  db:
    addr: 
      - "ip:port"
    username: ""
    password: ""
    db: 0
    poolSize: 0
```

### Deploy

```go
app.Deply(redis.Service())
```

### Use service client

```go
// get
result, err := redis.Get(ctx, key)

```
