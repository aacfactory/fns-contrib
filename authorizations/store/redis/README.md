# Authorizations Redis Store

## Usage

Make sure that redis service has been deployed.

```go
import (
    "github.com/aacfactory/fns-contrib/authorizations/store/redis"
)

authorizations.Service(redis.Component())
```
