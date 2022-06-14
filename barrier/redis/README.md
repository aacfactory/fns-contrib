# Redis Barrier

## Install
```shell
go get github.com/aacfactory/fns-contrib/barrier/redis
```
## Usage
make sure that redis service has been deployed.
```go
import (
	rb "github.com/aacfactory/fns-contrib/barrier/redis"
	"github.com/aacfactory/fns-contrib/database/redis"
)

func main() {

    app := fns.New(
        fns.Barrier(rb.Barrier())
    )

    deployErr := app.Deploy(redis.Service())
	
}
```