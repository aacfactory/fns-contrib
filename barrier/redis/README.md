# Redis Barrier

## Install
```shell
go get github.com/aacfactory/fns-contrib/barrier/redis
```
## Usage
Add redis barrier in `main.go`
```go
import (
	"github.com/aacfactory/fns-contrib/barrier/redis"
)

func main() {
    app := fns.New(
        fns.Barrier(redis.Barrier())
    )
}
```
Add redis service in `modules/dependencies.go`
```go
import (
	"github.com/aacfactory/fns-contrib/database/redis"
)

func dependencies() (services []service.Service) {
    services = append(services, redis.Service())
    return
}

```