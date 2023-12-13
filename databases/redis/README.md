# REDIS

One redis service for fns

## Install

```shell
go get github.com/aacfactory/fns-contrib/databases/redis
```

## Usage

### Config
Example
```yaml
redis:
  initAddress:
    - "ip:port"
  username: ""
  password: ""
```
Note: see [rueidis](https://github.com/redis/rueidis) for more details
### Deploy

```go
app.Deply(redis.New())
```

### Use service client

```go
// set
_, setErr := redis.Do(ctx, redis.Set("some", time.Now().Format(time.RFC3339)).Ex(10*time.Second))
if setErr != nil {
    return
}

// get
r, getErr := redis.Do(ctx, redis.Get("some"))
if getErr != nil {
    // 
    return
}
if r.IsNil() {  
	// 
	return
}
s, sErr := r.AsString()
if sErr != nil {
	return
}
```

## Cluster
Register cluster
```go
imports (
	_ "github.com/aacfactory/fns-contrib/databases/redis/clusters"
)
```
Setup config
```yaml
cluster:
  name: "redis"
  option:
    initAddress:
      - "ip:port"
    username: ""
    password: ""
    keepAlive:
      ttl: "60s"
      interval: "10s"
    barrier:
      ttl: "5s"
```
Note: it will use it owned barrier steadof common cluster barrier.