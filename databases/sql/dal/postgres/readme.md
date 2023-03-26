# Postgres

## Usage
Install
```go
import (
    _ "github.com/aacfactory/fns-contrib/databases/sql/dal/postgres"
)
```

## SEQUENCE
create sequence
```sql
CREATE SEQUENCE IF NOT EXISTS "FNS"."POST_COMMENT_ID"
    INCREMENT 1
    START 1
    MINVALUE 1
    MAXVALUE 9223372036854775807
    CACHE 1;

ALTER SEQUENCE "FNS"."POST_COMMENT_ID"
    OWNER TO aacfactory;
```
use
```go
dal.SequenceNextValue(ctx, `"FNS"."POST_COMMENT_ID"`)
```
