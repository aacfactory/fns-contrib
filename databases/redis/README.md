# REDIS

One redis service for fns

## Install

```shell
go get github.com/aacfactory/fns-contrib/databases/redis
```

## Usage

### Config

* Standalone type
    * masterSlaverMode = false
    * addr size is one
* Master slaver type
    * masterSlaverMode = true
    * first of addr is master, afters are slavers
* Cluster type
    * masterSlaverMode = false
    * all in addr is members

Example
```yaml
redis:
  masterSlaverMode: false
  network: "tcp"
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
