# Postgres

## Usage

Add import in deploy src file.
```go
import (
	_ "github.com/aacfactory/fns-contrib/databases/sql/dac/postgres/dialect"
)
```

Use `github.com/aacfactory/fns-contrib/databases/sql/dac/postgres` insteadof `github.com/aacfactory/fns-contrib/databases/sql/dac`.
```go

entry, err = postgres.Insert[Table](ctx, entry)

```
