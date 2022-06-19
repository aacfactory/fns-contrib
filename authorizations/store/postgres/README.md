# Authorizations Postgres Store

## Usage

Make sure that sql service has been deployed.

```go
import (
_ "github.com/aacfactory/fns-contrib/authorizations/store/postgres"
)
```

Config setting

```yaml
authorizations:
  store:
    schema: "schema"
    table: "table name"
```

DML

```sql
CREATE TABLE "YOUR SCHEMA"."YOUR TABLE"
(
    "ID"         character varying(63) NOT NULL PRIMARY KEY,
    "USER_ID"    character varying(63) NOT NULL,
    "NOT_BEFORE" timestamp without time zone NOT NULL DEFAULT 0,
    "NOT_AFTER"  timestamp without time zone NOT NULL,
    "VALUE"      text                  NOT NULL
) TABLESPACE pg_default;

ALTER TABLE IF EXISTS "YOUR SCHEMA"."YOUR TABLE" OWNER to someone;
```